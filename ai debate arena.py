import os
import time
import asyncio
import hashlib
import logging
import re
import secrets
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from collections import deque

from fastapi import FastAPI, HTTPException, Request, Response, Cookie
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field, field_validator, EmailStr
from dotenv import load_dotenv
import httpx

load_dotenv()

MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", 150))
MAX_QUERY_LENGTH = int(os.getenv("MAX_QUERY_LENGTH", 2000))
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", 30))
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", 60))
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:8000").split(",")
SESSION_EXPIRE_HOURS = 24

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
CHUTES_API_KEY = os.getenv("CHUTES_API_KEY", "").strip()
BYTEZ_API_KEY = os.getenv("BYTEZ_API_KEY", "").strip()
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "").strip()

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/auth/google/callback").strip()

try:
    from tavily import TavilyClient
    TAVILY_AVAILABLE = True
except ImportError:
    TAVILY_AVAILABLE = False

try:
    from bytez import Bytez
    BYTEZ_AVAILABLE = True
except ImportError as e:
    print(f"Bytez import failed: {e}")
    BYTEZ_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('backend.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

USERS_FILE = "users.json"
users_db: Dict[str, Dict] = {}
sessions: Dict[str, Dict] = {}

def load_users():
    global users_db
    try:
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, 'r') as f:
                users_db = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load users: {e}")

def save_users():
    try:
        with open(USERS_FILE, 'w') as f:
            json.dump(users_db, f)
    except Exception as e:
        logger.error(f"Failed to save users: {e}")

import bcrypt

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())
    except Exception:
        return False

def create_session(user_id: str) -> str:
    session_id = secrets.token_urlsafe(32)
    sessions[session_id] = {
        "user_id": user_id,
        "created": time.time(),
        "expires": time.time() + (SESSION_EXPIRE_HOURS * 3600)
    }
    return session_id

def validate_session(session_id: str) -> Optional[str]:
    if not session_id or session_id not in sessions:
        return None
    session = sessions[session_id]
    if time.time() > session["expires"]:
        del sessions[session_id]
        return None
    return session["user_id"]

load_users()

DANGEROUS_PATTERNS = [r'<script[^>]*>', r'javascript:', r'on\w+\s*=', r'data:text/html']

def sanitize_input(text: str) -> str:
    text = ''.join(c for c in text if ord(c) >= 32 or c in '\n\r\t')
    for pattern in DANGEROUS_PATTERNS:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    return text.strip()[:MAX_QUERY_LENGTH]

def get_client_id(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return hashlib.sha256(forwarded.split(",")[0].strip().encode()).hexdigest()[:16]
    return hashlib.sha256((request.client.host if request.client else "unknown").encode()).hexdigest()[:16]

PROVIDER_CONFIG = {
    "groq": {
        "base_url": "https://api.groq.com/openai/v1/chat/completions",
        "model": "llama-3.3-70b-versatile",
        "display_name": "Groq - Llama 3.3 70B",
        "color": "#f97316",
        "timeout": 30,
        "max_tokens": 150,
        "use_sdk": False
    },
    "openrouter": {
        "base_url": "https://openrouter.ai/api/v1/chat/completions",
        "model": "deepseek/deepseek-r1-0528:free",
        "display_name": "OpenRouter - DeepSeek R1",
        "color": "#8b5cf6",
        "timeout": 120,
        "max_tokens": 150,
        "use_sdk": False
    },
    "chutes": {
        "base_url": "https://api.chutes.ai/v1/chat/completions",
        "model": "deepseek-ai/DeepSeek-V3",
        "display_name": "Chutes - DeepSeek V3",
        "color": "#06b6d4",
        "timeout": 60,
        "max_tokens": 150,
        "use_sdk": False
    },
    "bytez": {
        "base_url": None,
        "model": "meta-llama/Llama-3.1-8B-Instruct",
        "display_name": "Bytez - Llama 3.1 8B",
        "color": "#10b981",
        "timeout": 60,
        "max_tokens": 150,
        "use_sdk": True
    }
}

class RateLimiter:
    def __init__(self, max_requests: int, window: int):
        self.max_requests = max_requests
        self.window = window
        self.requests: Dict[str, deque] = {}
        self.blocked: Dict[str, float] = {}  # Blocked IPs
    
    def is_allowed(self, client_id: str) -> bool:
        now = time.time()
        
        if client_id in self.blocked:
            if now < self.blocked[client_id]:
                return False
            del self.blocked[client_id]
        
        if client_id not in self.requests:
            self.requests[client_id] = deque()
        
        while self.requests[client_id] and self.requests[client_id][0] < now - self.window:
            self.requests[client_id].popleft()
        
        if len(self.requests[client_id]) >= self.max_requests:
            self.blocked[client_id] = now + self.window  # Block for window duration
            logger.warning(f"Rate limit exceeded for {client_id}")
            return False
        
        self.requests[client_id].append(now)
        return True
    
    def get_remaining(self, client_id: str) -> int:
        now = time.time()
        if client_id not in self.requests:
            return self.max_requests
        while self.requests[client_id] and self.requests[client_id][0] < now - self.window:
            self.requests[client_id].popleft()
        return max(0, self.max_requests - len(self.requests[client_id]))

rate_limiter = RateLimiter(RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW)

class TavilySearch:
    def __init__(self):
        self.client = None
        if TAVILY_AVAILABLE and TAVILY_API_KEY:
            try:
                self.client = TavilyClient(api_key=TAVILY_API_KEY)
                logger.info("Tavily initialized")
            except Exception as e:
                logger.error(f"Tavily init error: {e}")
    
    def is_available(self) -> bool:
        return self.client is not None
    
    async def search(self, query: str, max_results: int = 3) -> Dict[str, Any]:
        if not self.client:
            return {"error": "Not configured", "sources": [], "answer": None}
        
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, lambda: self.client.search(
                query=query[:500],  # Limit query length
                search_depth="basic",  # Faster, fewer tokens
                max_results=max_results,
                include_answer=True
            ))
            
            sources = []
            for r in result.get("results", [])[:max_results]:
                sources.append({
                    "title": r.get("title", "")[:100],
                    "url": r.get("url", ""),
                    "content": r.get("content", "")[:300]  # Limit content
                })
            
            return {
                "answer": result.get("answer", "")[:500],  # Limit answer
                "sources": sources,
                "query": query[:100]
            }
        except Exception as e:
            logger.error(f"Tavily error: {e}")
            return {"error": str(e), "sources": [], "answer": None}

tavily_search = TavilySearch()

class AskRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=MAX_QUERY_LENGTH)
    providers: Optional[List[str]] = Field(default=None)
    use_research: bool = Field(default=True)
    
    @field_validator('question')
    @classmethod
    def validate_question(cls, v: str) -> str:
        return sanitize_input(v)

class DebateResponse(BaseModel):
    request_id: str
    question: str
    research: Optional[Dict[str, Any]] = None
    responses: Dict[str, Dict[str, Any]]
    final_answer: Optional[str] = None
    processing_time_ms: float
    providers_used: List[str]
    tokens_used: int = 0

class DebateClient:
    def __init__(self):
        self.http_client: Optional[httpx.AsyncClient] = None
        self.stats = {p: {"requests": 0, "tokens": 0, "errors": 0, "latency_history": []} for p in PROVIDER_CONFIG}
        self.bytez_client = None
        if BYTEZ_AVAILABLE and BYTEZ_API_KEY:
            try:
                self.bytez_client = Bytez(BYTEZ_API_KEY)
                logger.info("Bytez initialized")
            except Exception as e:
                logger.error(f"Bytez error: {e}")
    
    async def get_client(self) -> httpx.AsyncClient:
        if self.http_client is None or self.http_client.is_closed:
            self.http_client = httpx.AsyncClient(timeout=120)
        return self.http_client
    
    async def close(self):
        if self.http_client:
            await self.http_client.aclose()
    
    def _get_api_key(self, provider: str) -> Optional[str]:
        keys = {"groq": GROQ_API_KEY, "openrouter": OPENROUTER_API_KEY, "chutes": CHUTES_API_KEY, "bytez": BYTEZ_API_KEY}
        key = keys.get(provider, "")
        return key if key else None
    
    def _get_headers(self, provider: str) -> Dict[str, str]:
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self._get_api_key(provider)}"}
        if provider == "openrouter":
            headers["HTTP-Referer"] = "http://localhost:8000"
            headers["X-Title"] = "AI Debate"
        return headers
    
    def get_available_providers(self) -> List[str]:
        available = []
        for p in PROVIDER_CONFIG:
            if p == "bytez":
                if BYTEZ_API_KEY and (BYTEZ_AVAILABLE or self.bytez_client):
                    available.append(p)
            elif self._get_api_key(p):
                available.append(p)
        return available
    
    async def call_provider(self, provider: str, question: str) -> Dict[str, Any]:
        config = PROVIDER_CONFIG[provider]
        start = time.time()
        
        try:
            if config.get("use_sdk") and provider == "bytez":
                return await self._call_bytez(question, config, start)
            
            client = await self.get_client()
            messages = [
                {"role": "system", "content": "Be concise. Max 100 words."},
                {"role": "user", "content": question[:1500]}  # Limit input
            ]
            response = await client.post(
                config["base_url"],
                headers=self._get_headers(provider),
                json={"model": config["model"], "messages": messages, "max_tokens": config["max_tokens"], "temperature": 0.7},
                timeout=config["timeout"]
            )
            latency = (time.time() - start) * 1000
            
            if response.status_code != 200:
                raise Exception(f"API Error: {response.status_code}")
            
            result = response.json()
            text = result.get("choices", [{}])[0].get("message", {}).get("content", "No response")
            tokens = result.get("usage", {}).get("total_tokens", len(text.split()))
            
            self.stats[provider]["requests"] += 1
            self.stats[provider]["tokens"] += tokens
            self.stats[provider]["latency_history"].append({"time": time.time(), "latency": latency})
            self.stats[provider]["latency_history"] = self.stats[provider]["latency_history"][-20:]  # Keep last 20
            
            return {"success": True, "response": text, "model": config["model"], "display_name": config["display_name"],
                    "color": config["color"], "latency_ms": round(latency, 2), "tokens": tokens}
        except Exception as e:
            self.stats[provider]["errors"] += 1
            logger.error(f"[{provider}] Error: {e}")
            return {"success": False, "error": str(e), "display_name": config["display_name"], "color": config["color"]}
    
    async def _call_bytez(self, question: str, config: Dict, start: float) -> Dict[str, Any]:
        if not self.bytez_client:
            raise Exception("Bytez not initialized")
        
        loop = asyncio.get_event_loop()
        model = self.bytez_client.model(config["model"])
        messages = [{"role": "user", "content": question[:1000]}]
        
        output, error = await loop.run_in_executor(None, lambda: model.run(messages))
        latency = (time.time() - start) * 1000
        
        if error:
            raise Exception(f"Bytez: {error}")
        
        text = output if isinstance(output, str) else str(output)
        tokens = len(text.split())
        self.stats["bytez"]["requests"] += 1
        self.stats["bytez"]["tokens"] += tokens
        self.stats["bytez"]["latency_history"].append({"time": time.time(), "latency": latency})
        
        return {"success": True, "response": text, "model": config["model"], "display_name": config["display_name"],
                "color": config["color"], "latency_ms": round(latency, 2), "tokens": tokens}
    
    async def debate(self, question: str, providers: List[str] = None, research_context: str = None) -> Dict[str, Dict[str, Any]]:
        available = providers or self.get_available_providers()
        if not available:
            raise HTTPException(503, "No providers")
        
        if research_context:
            enhanced = f"Context: {research_context[:800]}\n\nQ: {question}\n\nBe concise."
        else:
            enhanced = question
        
        tasks = {p: self.call_provider(p, enhanced) for p in available if p in self.get_available_providers()}
        return {p: await t for p, t in tasks.items()}
    
    def synthesize_answer(self, responses: Dict[str, Dict], research: Dict = None) -> str:
        successful = [(n, r) for n, r in responses.items() if r.get("success")]
        if not successful:
            return "No responses available."
        
        if len(successful) == 1:
            return successful[0][1]["response"]
        
        parts = [f"**{r['display_name']}:** {r['response'][:400]}" for _, r in successful]
        combined = "\n\n".join(parts)
        
        if research and research.get("answer"):
            return f"**Research:** {research['answer'][:300]}\n\n{combined}"
        return combined

debate_client = DebateClient()

class LoginRequest(BaseModel):
    email: str = Field(..., min_length=3, max_length=100)
    password: str = Field(..., min_length=6, max_length=100)

class SignupRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=50)
    email: str = Field(..., min_length=3, max_length=100)
    password: str = Field(..., min_length=6, max_length=100)

LOGIN_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login - AI Debate Arena</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, sans-serif;
            background: linear-gradient(135deg, #0a0a0a 0%, #1a1a2e 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 1rem;
        }
        .auth-container {
            background: #141414;
            border-radius: 20px;
            padding: 2.5rem;
            width: 100%;
            max-width: 400px;
            border: 1px solid #2a2a2a;
            box-shadow: 0 20px 60px rgba(0,0,0,0.5);
        }
        .logo {
            text-align: center;
            margin-bottom: 2rem;
        }
        .logo h1 {
            color: #fff;
            font-size: 1.75rem;
            margin-bottom: 0.5rem;
        }
        .logo p {
            color: #888;
            font-size: 0.9rem;
        }
        .form-group {
            margin-bottom: 1.25rem;
        }
        label {
            display: block;
            color: #888;
            font-size: 0.85rem;
            margin-bottom: 0.5rem;
        }
        .input-wrapper {
            position: relative;
        }
        input {
            width: 100%;
            padding: 0.875rem 1rem;
            background: #0a0a0a;
            border: 1px solid #2a2a2a;
            border-radius: 10px;
            color: #fff;
            font-size: 1rem;
            transition: border-color 0.2s;
        }
        input:focus {
            outline: none;
            border-color: #3b82f6;
        }
        .password-toggle {
            position: absolute;
            right: 1rem;
            top: 50%;
            transform: translateY(-50%);
            background: none;
            border: none;
            color: #888;
            cursor: pointer;
            font-size: 0.85rem;
        }
        .password-toggle:hover {
            color: #fff;
        }
        .btn {
            width: 100%;
            padding: 0.875rem;
            border: none;
            border-radius: 10px;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: transform 0.2s, opacity 0.2s;
        }
        .btn:hover {
            transform: translateY(-1px);
        }
        .btn-primary {
            background: linear-gradient(135deg, #3b82f6, #2563eb);
            color: #fff;
        }
        .btn-google {
            background: #fff;
            color: #333;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.75rem;
            margin-top: 1rem;
        }
        .divider {
            display: flex;
            align-items: center;
            margin: 1.5rem 0;
            color: #666;
            font-size: 0.85rem;
        }
        .divider::before, .divider::after {
            content: "";
            flex: 1;
            border-bottom: 1px solid #2a2a2a;
        }
        .divider::before { margin-right: 1rem; }
        .divider::after { margin-left: 1rem; }
        .switch-auth {
            text-align: center;
            margin-top: 1.5rem;
            color: #888;
            font-size: 0.9rem;
        }
        .switch-auth a {
            color: #3b82f6;
            text-decoration: none;
            font-weight: 500;
        }
        .switch-auth a:hover {
            text-decoration: underline;
        }
        .error-msg {
            background: rgba(248,113,113,0.1);
            color: #f87171;
            padding: 0.75rem;
            border-radius: 8px;
            margin-bottom: 1rem;
            font-size: 0.85rem;
            display: none;
        }
        .error-msg.show {
            display: block;
        }
        .google-icon {
            width: 20px;
            height: 20px;
        }
    </style>
</head>
<body>
    <div class="auth-container">
        <div class="logo">
            <h1>AI Debate Arena</h1>
            <p>Sign in to continue</p>
        </div>
        
        <div id="error-msg" class="error-msg"></div>
        
        <form id="login-form" onsubmit="handleLogin(event)">
            <div class="form-group">
                <label for="email">Email</label>
                <input type="email" id="email" placeholder="Enter your email" required>
            </div>
            
            <div class="form-group">
                <label for="password">Password</label>
                <div class="input-wrapper">
                    <input type="password" id="password" placeholder="Enter your password" required minlength="6">
                    <button type="button" class="password-toggle" onclick="togglePassword()">Show</button>
                </div>
            </div>
            
            <button type="submit" class="btn btn-primary" id="login-btn">Sign In</button>
        </form>
        
        <div class="divider">or continue with</div>
        
        <button class="btn btn-google" onclick="handleGoogleLogin()">
            <svg class="google-icon" viewBox="0 0 24 24"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg>
            Continue with Google
        </button>
        
        <div class="switch-auth">
            Don't have an account? <a href="/signup">Sign up</a>
        </div>
    </div>
    
    <script>
        function togglePassword() {
            const pwd = document.getElementById('password');
            const btn = document.querySelector('.password-toggle');
            if (pwd.type === 'password') {
                pwd.type = 'text';
                btn.textContent = 'Hide';
            } else {
                pwd.type = 'password';
                btn.textContent = 'Show';
            }
        }
        
        async function handleLogin(e) {
            e.preventDefault();
            const btn = document.getElementById('login-btn');
            const errorDiv = document.getElementById('error-msg');
            
            btn.disabled = true;
            btn.textContent = 'Signing in...';
            errorDiv.classList.remove('show');
            
            try {
                const res = await fetch('/api/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        email: document.getElementById('email').value,
                        password: document.getElementById('password').value
                    })
                });
                
                const data = await res.json();
                
                if (res.ok) {
                    window.location.href = '/app';
                } else {
                    errorDiv.textContent = data.detail || 'Login failed';
                    errorDiv.classList.add('show');
                }
            } catch (e) {
                errorDiv.textContent = 'Connection error. Please try again.';
                errorDiv.classList.add('show');
            } finally {
                btn.disabled = false;
                btn.textContent = 'Sign In';
            }
        }
        
        function handleGoogleLogin() {
            window.location.href = '/auth/google';
        }
        
        // Check for error in URL
        const urlParams = new URLSearchParams(window.location.search);
        if (urlParams.get('error')) {
            document.getElementById('error-msg').textContent = 'Google login failed. Please try again.';
            document.getElementById('error-msg').classList.add('show');
        }
    </script>
</body>
</html>
'''

SIGNUP_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sign Up - AI Debate Arena</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, sans-serif;
            background: linear-gradient(135deg, #0a0a0a 0%, #1a1a2e 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 1rem;
        }
        .auth-container {
            background: #141414;
            border-radius: 20px;
            padding: 2.5rem;
            width: 100%;
            max-width: 400px;
            border: 1px solid #2a2a2a;
            box-shadow: 0 20px 60px rgba(0,0,0,0.5);
        }
        .logo { text-align: center; margin-bottom: 2rem; }
        .logo h1 { color: #fff; font-size: 1.75rem; margin-bottom: 0.5rem; }
        .logo p { color: #888; font-size: 0.9rem; }
        .form-group { margin-bottom: 1.25rem; }
        label { display: block; color: #888; font-size: 0.85rem; margin-bottom: 0.5rem; }
        .input-wrapper { position: relative; }
        input {
            width: 100%;
            padding: 0.875rem 1rem;
            background: #0a0a0a;
            border: 1px solid #2a2a2a;
            border-radius: 10px;
            color: #fff;
            font-size: 1rem;
        }
        input:focus { outline: none; border-color: #3b82f6; }
        .password-toggle {
            position: absolute; right: 1rem; top: 50%; transform: translateY(-50%);
            background: none; border: none; color: #888; cursor: pointer; font-size: 0.85rem;
        }
        .btn {
            width: 100%; padding: 0.875rem; border: none; border-radius: 10px;
            font-size: 1rem; font-weight: 600; cursor: pointer;
        }
        .btn-primary { background: linear-gradient(135deg, #10b981, #059669); color: #fff; }
        .btn-google {
            background: #fff; color: #333; display: flex; align-items: center;
            justify-content: center; gap: 0.75rem; margin-top: 1rem;
        }
        .divider {
            display: flex; align-items: center; margin: 1.5rem 0; color: #666; font-size: 0.85rem;
        }
        .divider::before, .divider::after { content: ""; flex: 1; border-bottom: 1px solid #2a2a2a; }
        .divider::before { margin-right: 1rem; }
        .divider::after { margin-left: 1rem; }
        .switch-auth { text-align: center; margin-top: 1.5rem; color: #888; font-size: 0.9rem; }
        .switch-auth a { color: #3b82f6; text-decoration: none; font-weight: 500; }
        .error-msg {
            background: rgba(248,113,113,0.1); color: #f87171; padding: 0.75rem;
            border-radius: 8px; margin-bottom: 1rem; font-size: 0.85rem; display: none;
        }
        .error-msg.show { display: block; }
        .success-msg {
            background: rgba(16,185,129,0.1); color: #10b981; padding: 0.75rem;
            border-radius: 8px; margin-bottom: 1rem; font-size: 0.85rem; display: none;
        }
        .success-msg.show { display: block; }
    </style>
</head>
<body>
    <div class="auth-container">
        <div class="logo">
            <h1>Create Account</h1>
            <p>Join AI Debate Arena</p>
        </div>
        
        <div id="error-msg" class="error-msg"></div>
        <div id="success-msg" class="success-msg"></div>
        
        <form id="signup-form" onsubmit="handleSignup(event)">
            <div class="form-group">
                <label for="name">Full Name</label>
                <input type="text" id="name" placeholder="Enter your name" required minlength="2">
            </div>
            
            <div class="form-group">
                <label for="email">Email</label>
                <input type="email" id="email" placeholder="Enter your email" required>
            </div>
            
            <div class="form-group">
                <label for="password">Password</label>
                <div class="input-wrapper">
                    <input type="password" id="password" placeholder="Create a password (min 6 chars)" required minlength="6">
                    <button type="button" class="password-toggle" onclick="togglePassword()">Show</button>
                </div>
            </div>
            
            <button type="submit" class="btn btn-primary" id="signup-btn">Create Account</button>
        </form>
        
        <div class="divider">or continue with</div>
        
        <button class="btn btn-google" onclick="handleGoogleSignup()">
            <svg width="20" height="20" viewBox="0 0 24 24"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg>
            Continue with Google
        </button>
        
        <div class="switch-auth">
            Already have an account? <a href="/login">Sign in</a>
        </div>
    </div>
    
    <script>
        function togglePassword() {
            const pwd = document.getElementById('password');
            const btn = document.querySelector('.password-toggle');
            pwd.type = pwd.type === 'password' ? 'text' : 'password';
            btn.textContent = pwd.type === 'password' ? 'Show' : 'Hide';
        }
        
        async function handleSignup(e) {
            e.preventDefault();
            const btn = document.getElementById('signup-btn');
            const errorDiv = document.getElementById('error-msg');
            const successDiv = document.getElementById('success-msg');
            
            btn.disabled = true;
            btn.textContent = 'Creating account...';
            errorDiv.classList.remove('show');
            successDiv.classList.remove('show');
            
            try {
                const res = await fetch('/api/signup', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        name: document.getElementById('name').value,
                        email: document.getElementById('email').value,
                        password: document.getElementById('password').value
                    })
                });
                
                const data = await res.json();
                
                if (res.ok) {
                    successDiv.textContent = 'Account created! Redirecting...';
                    successDiv.classList.add('show');
                    setTimeout(() => window.location.href = '/app', 1500);
                } else {
                    errorDiv.textContent = data.detail || 'Signup failed';
                    errorDiv.classList.add('show');
                }
            } catch (e) {
                errorDiv.textContent = 'Connection error';
                errorDiv.classList.add('show');
            } finally {
                btn.disabled = false;
                btn.textContent = 'Create Account';
            }
        }
        
        function handleGoogleSignup() {
            window.location.href = '/auth/google';
        }
    </script>
</body>
</html>
'''

HTML_CONTENT = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Security-Policy" content="default-src 'self'; script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; style-src 'self' 'unsafe-inline'; connect-src 'self';">
    <title>AI Debate Arena</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        :root { --bg: #0a0a0a; --surface: #141414; --surface-hover: #1f1f1f; --border: #2a2a2a; --text: #fff; --text-muted: #888; --accent: #3b82f6; }
        [data-theme="light"] { --bg: #f5f5f5; --surface: #fff; --surface-hover: #f0f0f0; --border: #e0e0e0; --text: #1a1a1a; --text-muted: #666; }
        body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: var(--bg); color: var(--text); height: 100vh; display: flex; }
        .sidebar { width: 200px; background: var(--surface); border-right: 1px solid var(--border); padding: 1rem; }
        .logo { font-size: 1.1rem; font-weight: 700; padding: 1rem; margin-bottom: 1rem; border-bottom: 1px solid var(--border); }
        .nav-item { padding: 0.75rem 1rem; border-radius: 8px; cursor: pointer; margin-bottom: 0.25rem; transition: 0.2s; }
        .nav-item:hover { background: var(--surface-hover); }
        .nav-item.active { background: var(--accent); color: #fff; }
        .main-content { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
        .content-area { flex: 1; padding: 1.5rem; overflow-y: auto; }
        .hidden { display: none !important; }
        .input-section { background: var(--surface); border-radius: 12px; padding: 1.25rem; margin-bottom: 1.5rem; border: 1px solid var(--border); }
        .input-container { display: flex; gap: 0.75rem; }
        textarea { flex: 1; background: var(--bg); border: 1px solid var(--border); border-radius: 8px; padding: 0.875rem; color: var(--text); font-size: 0.95rem; resize: none; min-height: 60px; }
        textarea:focus { outline: none; border-color: var(--accent); }
        button { background: var(--accent); color: #fff; border: none; padding: 0.75rem 1.25rem; border-radius: 8px; font-weight: 600; cursor: pointer; }
        button:disabled { opacity: 0.5; }
        .debate-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 1rem; }
        .response-card { background: var(--surface); border-radius: 12px; padding: 1rem; border: 1px solid var(--border); }
        .response-header { display: flex; justify-content: space-between; margin-bottom: 0.75rem; padding-bottom: 0.5rem; border-bottom: 1px solid var(--border); }
        .provider-name { font-weight: 600; font-size: 0.9rem; }
        .response-meta { font-size: 0.7rem; color: var(--text-muted); }
        .response-content { line-height: 1.5; font-size: 0.85rem; white-space: pre-wrap; }
        .response-error { color: #f87171; background: rgba(248,113,113,0.1); padding: 0.5rem; border-radius: 6px; font-size: 0.8rem; }
        .loading { display: flex; align-items: center; gap: 0.5rem; color: var(--text-muted); font-size: 0.85rem; }
        .spinner { width: 16px; height: 16px; border: 2px solid var(--border); border-top-color: var(--accent); border-radius: 50%; animation: spin 1s linear infinite; }
        @keyframes spin { to { transform: rotate(360deg); } }
        .settings-section { max-width: 500px; }
        .settings-group { background: var(--surface); border-radius: 12px; padding: 1rem; border: 1px solid var(--border); margin-bottom: 1rem; }
        .settings-group h3 { font-size: 0.9rem; margin-bottom: 0.75rem; padding-bottom: 0.5rem; border-bottom: 1px solid var(--border); }
        .setting-item { display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; }
        .toggle { position: relative; width: 40px; height: 22px; }
        .toggle input { opacity: 0; width: 0; height: 0; }
        .toggle-slider { position: absolute; cursor: pointer; inset: 0; background: var(--border); border-radius: 22px; transition: 0.3s; }
        .toggle-slider:before { content: ""; position: absolute; height: 16px; width: 16px; left: 3px; bottom: 3px; background: #fff; border-radius: 50%; transition: 0.3s; }
        .toggle input:checked + .toggle-slider { background: var(--accent); }
        .toggle input:checked + .toggle-slider:before { transform: translateX(18px); }
        .provider-toggle { display: flex; align-items: center; gap: 0.75rem; padding: 0.4rem 0; }
        .provider-dot { width: 8px; height: 8px; border-radius: 50%; }
        .history-item { background: var(--surface); border-radius: 10px; padding: 0.75rem; border: 1px solid var(--border); cursor: pointer; margin-bottom: 0.5rem; }
        .history-item:hover { background: var(--surface-hover); }
        .chart-container { height: 300px; background: var(--surface); border-radius: 12px; padding: 1rem; border: 1px solid var(--border); margin-bottom: 1rem; }
        .stats-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin-bottom: 1rem; }
        .stat-card { background: var(--surface); border-radius: 10px; padding: 1rem; text-align: center; border: 1px solid var(--border); }
        .stat-value { font-size: 1.5rem; font-weight: 700; }
        .stat-label { font-size: 0.7rem; color: var(--text-muted); }
        @media (max-width: 768px) { .sidebar { width: 60px; } .sidebar .logo span, .nav-item span { display: none; } .stats-row { grid-template-columns: repeat(2, 1fr); } }
    </style>
</head>
<body>
    <aside class="sidebar">
        <div class="logo"><span>AI Debate</span></div>
        <div class="nav-item active" data-tab="debate"><span>Debate</span></div>
        <div class="nav-item" data-tab="history"><span>History</span></div>
        <div class="nav-item" data-tab="analytics"><span>Analytics</span></div>
        <div class="nav-item" data-tab="settings"><span>Settings</span></div>
    </aside>
    
    <main class="main-content">
        <div class="content-area">
            <div id="debate-tab">
                <div class="input-section">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
                        <div>
                            <h2 style="font-size: 1.1rem;">Ask a Question</h2>
                            <p style="font-size: 0.8rem; color: var(--text-muted);">Models research and debate your question</p>
                        </div>
                        <div style="display: flex; align-items: center; gap: 0.5rem;">
                            <span style="font-size: 0.75rem; color: var(--text-muted);">Research</span>
                            <label class="toggle"><input type="checkbox" id="research-toggle" checked><span class="toggle-slider"></span></label>
                        </div>
                    </div>
                    <div class="input-container">
                        <textarea id="question" placeholder="What would you like to research?" maxlength="2000"></textarea>
                        <button id="submit-btn" onclick="submitQuestion()">Debate</button>
                    </div>
                    <div style="font-size: 0.7rem; color: var(--text-muted); margin-top: 0.5rem;">
                        <span id="char-count">0</span>/2000 chars | Tokens saved: <span id="tokens-saved">0</span>
                    </div>
                </div>
                
                <div id="research-results" class="hidden" style="background: var(--surface); border-radius: 12px; padding: 1rem; margin-bottom: 1rem; border: 1px solid #22c55e44;">
                    <div style="color: #22c55e; font-weight: 600; margin-bottom: 0.5rem; font-size: 0.9rem;">Research</div>
                    <div id="research-answer" style="font-size: 0.85rem; line-height: 1.5;"></div>
                    <div id="research-sources" style="font-size: 0.75rem; color: var(--text-muted); margin-top: 0.5rem;"></div>
                </div>
                
                <div id="final-answer" class="hidden" style="background: linear-gradient(135deg, rgba(59,130,246,0.1), transparent); border-radius: 12px; padding: 1rem; margin-bottom: 1rem; border: 1px solid var(--accent);">
                    <div style="color: var(--accent); font-weight: 600; margin-bottom: 0.5rem; font-size: 0.9rem;">Summary</div>
                    <div id="final-answer-content" style="font-size: 0.85rem; line-height: 1.6; white-space: pre-wrap;"></div>
                </div>
                
                <h3 style="margin-bottom: 0.75rem; font-size: 0.9rem; opacity: 0.8;">Perspectives</h3>
                <div class="debate-grid" id="responses"></div>
            </div>
            
            <div id="history-tab" class="hidden">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                    <h2 style="font-size: 1.1rem;">History</h2>
                    <button onclick="clearHistory()" style="background: rgba(248,113,113,0.2); color: #f87171; padding: 0.4rem 0.75rem; font-size: 0.8rem;">Clear</button>
                </div>
                <div id="history-list"></div>
            </div>
            
            <div id="analytics-tab" class="hidden">
                <h2 style="font-size: 1.1rem; margin-bottom: 1rem;">Analytics</h2>
                <div class="stats-row" id="stats-cards"></div>
                <div class="chart-container">
                    <canvas id="latencyChart"></canvas>
                </div>
                <div class="chart-container">
                    <canvas id="tokensChart"></canvas>
                </div>
            </div>
            
            <div id="settings-tab" class="hidden settings-section">
                <h2 style="font-size: 1.1rem; margin-bottom: 1rem;">Settings</h2>
                <div class="settings-group">
                    <h3>Appearance</h3>
                    <div class="setting-item">
                        <span style="font-size: 0.85rem;">Dark Theme</span>
                        <label class="toggle"><input type="checkbox" id="theme-toggle" checked onchange="toggleTheme()"><span class="toggle-slider"></span></label>
                    </div>
                </div>
                <div class="settings-group">
                    <h3>Providers</h3>
                    <div id="provider-settings"></div>
                </div>
                <div class="settings-group">
                    <h3>Security</h3>
                    <div style="font-size: 0.8rem; color: var(--text-muted);">
                        <p>• Rate limit: 30 req/min</p>
                        <p>• Max query: 2000 chars</p>
                        <p>• Input sanitization: Active</p>
                        <p>• CSP headers: Enabled</p>
                    </div>
                </div>
                <div class="settings-group">
                    <h3>Account</h3>
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span id="user-display" style="font-size: 0.85rem; color: var(--text-muted);">...</span>
                        <button onclick="window.location.href='/api/logout'" style="background: rgba(248,113,113,0.15); color: #f87171; padding: 0.5rem 1rem; border-radius: 6px; font-size: 0.85rem; border: 1px solid rgba(248,113,113,0.3);">Sign Out</button>
                    </div>
                </div>
            </div>
        </div>
    </main>
    
    <script>
        let history = JSON.parse(localStorage.getItem('debate_history') || '[]');
        let settings = JSON.parse(localStorage.getItem('debate_settings') || '{"theme":"dark","providers":{"groq":true,"openrouter":true,"chutes":true,"bytez":true}}');
        let providers = {};
        let latencyChart = null;
        let tokensChart = null;
        let totalTokensSaved = parseInt(localStorage.getItem('tokens_saved') || '0');
        
        document.getElementById('tokens-saved').textContent = totalTokensSaved;
        
        document.getElementById('question').addEventListener('input', e => {
            document.getElementById('char-count').textContent = e.target.value.length;
        });
        
        document.querySelectorAll('.nav-item').forEach(item => {
            item.addEventListener('click', () => {
                document.querySelectorAll('.nav-item').forEach(i => i.classList.remove('active'));
                item.classList.add('active');
                const tab = item.dataset.tab;
                ['debate','history','analytics','settings'].forEach(t => document.getElementById(t + '-tab').classList.toggle('hidden', t !== tab));
                if (tab === 'history') renderHistory();
                if (tab === 'analytics') fetchStats();
                if (tab === 'settings') renderSettings();
            });
        });
        
        function toggleTheme() {
            settings.theme = document.getElementById('theme-toggle').checked ? 'dark' : 'light';
            document.documentElement.setAttribute('data-theme', settings.theme);
            localStorage.setItem('debate_settings', JSON.stringify(settings));
        }
        
        function applyTheme() {
            document.documentElement.setAttribute('data-theme', settings.theme);
            document.getElementById('theme-toggle').checked = settings.theme === 'dark';
        }
        
        async function fetchProviders() {
            try {
                const res = await fetch('/providers');
                providers = await res.json();
                renderSettings();
            } catch (e) { console.error(e); }
        }
        
        function renderSettings() {
            const container = document.getElementById('provider-settings');
            container.innerHTML = Object.entries(providers).filter(([n]) => n !== 'tavily').map(([name, info]) => `
                <div class="provider-toggle">
                    <label class="toggle">
                        <input type="checkbox" ${settings.providers[name] !== false && info.available ? 'checked' : ''} ${!info.available ? 'disabled' : ''} onchange="toggleProvider('${name}')">
                        <span class="toggle-slider"></span>
                    </label>
                    <div class="provider-dot" style="background: ${info.color}"></div>
                    <span style="font-size: 0.85rem;">${info.display_name}</span>
                </div>
            `).join('');
        }
        
        function toggleProvider(name) {
            settings.providers[name] = !settings.providers[name];
            localStorage.setItem('debate_settings', JSON.stringify(settings));
        }
        
        function getSelectedProviders() {
            return Object.entries(settings.providers).filter(([n, v]) => v && providers[n]?.available && n !== 'tavily').map(([n]) => n);
        }
        
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        async function submitQuestion() {
            const textarea = document.getElementById('question');
            const question = textarea.value.trim();
            if (!question) return;
            
            const btn = document.getElementById('submit-btn');
            const selected = getSelectedProviders();
            const useResearch = document.getElementById('research-toggle').checked;
            
            if (selected.length === 0) { alert('Select at least one provider'); return; }
            
            btn.disabled = true;
            btn.textContent = 'Working...';
            document.getElementById('research-results').classList.add('hidden');
            document.getElementById('final-answer').classList.add('hidden');
            
            document.getElementById('responses').innerHTML = selected.map(name => `
                <div class="response-card" style="border-left: 3px solid ${providers[name].color}">
                    <div class="response-header"><div class="provider-name">${providers[name].display_name}</div></div>
                    <div class="loading"><div class="spinner"></div> Processing...</div>
                </div>
            `).join('');
            
            try {
                const res = await fetch('/debate', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ question, providers: selected, use_research: useResearch })
                });
                const data = await res.json();
                
                if (data.research?.answer) {
                    document.getElementById('research-answer').textContent = data.research.answer;
                    document.getElementById('research-sources').innerHTML = data.research.sources?.map(s => 
                        `<a href="${s.url}" target="_blank" rel="noopener" style="color: #22c55e; margin-right: 0.5rem;">${escapeHtml(s.title)}</a>`
                    ).join(' ') || '';
                    document.getElementById('research-results').classList.remove('hidden');
                }
                
                if (data.final_answer) {
                    document.getElementById('final-answer-content').textContent = data.final_answer;
                    document.getElementById('final-answer').classList.remove('hidden');
                }
                
                renderResponses(data.responses);
                
                // Track tokens
                totalTokensSaved += (2000 - question.length);
                localStorage.setItem('tokens_saved', totalTokensSaved);
                document.getElementById('tokens-saved').textContent = totalTokensSaved;
                
                history.unshift({ id: data.request_id, question, responses: data.responses, research: data.research, timestamp: Date.now(), processing_time: data.processing_time_ms, tokens: data.tokens_used });
                history = history.slice(0, 30);
                localStorage.setItem('debate_history', JSON.stringify(history));
            } catch (e) {
                document.getElementById('responses').innerHTML = `<div class="response-error">${e.message}</div>`;
            } finally {
                btn.disabled = false;
                btn.textContent = 'Debate';
            }
        }
        
        function renderResponses(responses) {
            document.getElementById('responses').innerHTML = Object.entries(responses).map(([name, data]) => `
                <div class="response-card" style="border-left: 3px solid ${data.color}">
                    <div class="response-header">
                        <div class="provider-name">${data.display_name}</div>
                        ${data.success ? `<div class="response-meta">${data.latency_ms}ms | ${data.tokens}t</div>` : ''}
                    </div>
                    ${data.success ? `<div class="response-content">${escapeHtml(data.response)}</div>` : `<div class="response-error">${data.error}</div>`}
                </div>
            `).join('');
        }
        
        function renderHistory() {
            const container = document.getElementById('history-list');
            if (!history.length) { container.innerHTML = '<p style="color: var(--text-muted); text-align: center; padding: 2rem;">No history</p>'; return; }
            container.innerHTML = history.map(item => `
                <div class="history-item" onclick='loadHistoryItem(${JSON.stringify(item).replace(/'/g, "\\'")})'>
                    <div style="font-weight: 500; font-size: 0.85rem; margin-bottom: 0.25rem;">${escapeHtml(item.question.substring(0, 80))}${item.question.length > 80 ? '...' : ''}</div>
                    <div style="font-size: 0.7rem; color: var(--text-muted);">${new Date(item.timestamp).toLocaleDateString()} | ${item.processing_time?.toFixed(0)}ms</div>
                </div>
            `).join('');
        }
        
        function loadHistoryItem(item) {
            document.querySelector('[data-tab="debate"]').click();
            document.getElementById('question').value = item.question;
            if (item.research?.answer) {
                document.getElementById('research-answer').textContent = item.research.answer;
                document.getElementById('research-results').classList.remove('hidden');
            }
            renderResponses(item.responses);
        }
        
        function clearHistory() {
            if (confirm('Clear history?')) {
                history = [];
                localStorage.removeItem('debate_history');
                renderHistory();
            }
        }
        
        async function fetchStats() {
            try {
                const res = await fetch('/stats');
                const data = await res.json();
                
                // Stats cards
                let totalReqs = 0, totalToks = 0, totalErrs = 0;
                Object.values(data.providers).forEach(s => { totalReqs += s.requests; totalToks += s.tokens; totalErrs += s.errors; });
                
                document.getElementById('stats-cards').innerHTML = `
                    <div class="stat-card"><div class="stat-value">${totalReqs}</div><div class="stat-label">Requests</div></div>
                    <div class="stat-card"><div class="stat-value">${totalToks}</div><div class="stat-label">Tokens</div></div>
                    <div class="stat-card"><div class="stat-value">${totalErrs}</div><div class="stat-label">Errors</div></div>
                    <div class="stat-card"><div class="stat-value">${Object.keys(data.providers).length}</div><div class="stat-label">Providers</div></div>
                `;
                
                // Latency line chart
                const labels = Object.keys(data.providers);
                const latencies = labels.map(p => data.providers[p].latency_history || []);
                
                if (latencyChart) latencyChart.destroy();
                latencyChart = new Chart(document.getElementById('latencyChart'), {
                    type: 'line',
                    data: {
                        labels: Array.from({length: 10}, (_, i) => `${i+1}`),
                        datasets: labels.map((name, i) => ({
                            label: name,
                            data: (latencies[i] || []).slice(-10).map(l => l.latency),
                            borderColor: data.providers[name].color,
                            backgroundColor: data.providers[name].color + '22',
                            fill: true,
                            tension: 0.4
                        }))
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { labels: { color: getComputedStyle(document.body).getPropertyValue('--text') } }, title: { display: true, text: 'Latency (ms)', color: getComputedStyle(document.body).getPropertyValue('--text') } },
                        scales: { y: { beginAtZero: true, grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#888' } }, x: { grid: { display: false }, ticks: { color: '#888' } } }
                    }
                });
                
                // Tokens bar chart
                if (tokensChart) tokensChart.destroy();
                tokensChart = new Chart(document.getElementById('tokensChart'), {
                    type: 'bar',
                    data: {
                        labels: labels,
                        datasets: [{ label: 'Tokens Used', data: labels.map(p => data.providers[p].tokens), backgroundColor: labels.map(p => data.providers[p].color) }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { display: false }, title: { display: true, text: 'Token Usage by Provider', color: getComputedStyle(document.body).getPropertyValue('--text') } },
                        scales: { y: { beginAtZero: true, grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#888' } }, x: { grid: { display: false }, ticks: { color: '#888' } } }
                    }
                });
            } catch (e) { console.error(e); }
        }
        
        document.getElementById('question').addEventListener('keydown', e => { if (e.key === 'Enter' && e.ctrlKey) submitQuestion(); });
        applyTheme();
        fetchProviders();
        fetchUser();
        
        async function fetchUser() {
            try {
                const res = await fetch('/api/me');
                if (res.ok) {
                    const data = await res.json();
                    const display = document.getElementById('user-display');
                    if (display) {
                        display.innerHTML = `<span style="color: var(--text); font-weight: 500;">${data.name}</span><br><span style="font-size: 0.75rem;">${data.email}</span>`;
                    }
                }
            } catch (e) {
                console.error('Failed to fetch user:', e);
            }
        }
    </script>
</body>
</html>
'''

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting AI Debate Arena...")
    logger.info(f"Providers: {debate_client.get_available_providers()}")
    logger.info(f"Tavily: {tavily_search.is_available()}")
    yield
    await debate_client.close()

app = FastAPI(title="AI Debate Arena", version="2.1.0", lifespan=lifespan, docs_url=None, redoc_url=None)

@app.middleware("http")
async def security_middleware(request: Request, call_next):
    ua = request.headers.get("user-agent", "").lower()
    if any(bot in ua for bot in ["sqlmap", "nikto", "nmap", "masscan", "curl"]):
        logger.warning(f"Blocked suspicious request: {ua[:50]}")
        return JSONResponse(status_code=403, content={"error": "Forbidden"})
    
    response = await call_next(request)
    
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Cache-Control"] = "no-store"
    
    return response

app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS, allow_methods=["GET", "POST"], allow_headers=["Content-Type"])

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    session_id = request.cookies.get("session_id")
    if validate_session(session_id):
        return RedirectResponse(url="/app", status_code=302)
    return RedirectResponse(url="/login", status_code=302)

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    session_id = request.cookies.get("session_id")
    if validate_session(session_id):
        return RedirectResponse(url="/app", status_code=302)
    return LOGIN_HTML

@app.get("/signup", response_class=HTMLResponse)
async def signup_page(request: Request):
    session_id = request.cookies.get("session_id")
    if validate_session(session_id):
        return RedirectResponse(url="/app", status_code=302)
    return SIGNUP_HTML

@app.get("/app", response_class=HTMLResponse)
async def app_page(request: Request):
    session_id = request.cookies.get("session_id")
    user_id = validate_session(session_id)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)
    return HTML_CONTENT

@app.post("/api/login")
async def api_login(body: LoginRequest, response: Response):
    email = body.email.lower().strip()
    if email not in users_db:
        raise HTTPException(401, detail="Invalid email or password")
    
    user = users_db[email]
    if not verify_password(body.password, user["password"]):
        raise HTTPException(401, detail="Invalid email or password")
    
    session_id = create_session(email)
    response.set_cookie(
        key="session_id",
        value=session_id,
        httponly=True,
        max_age=SESSION_EXPIRE_HOURS * 3600,
        samesite="lax"
    )
    logger.info(f"User logged in: {email}")
    return {"success": True, "user": {"email": email, "name": user["name"]}}

@app.post("/api/signup")
async def api_signup(body: SignupRequest, response: Response):
    email = body.email.lower().strip()
    if email in users_db:
        raise HTTPException(400, detail="Email already registered")
    
    users_db[email] = {
        "name": body.name.strip(),
        "password": hash_password(body.password),
        "created": time.time()
    }
    save_users()
    
    session_id = create_session(email)
    response.set_cookie(
        key="session_id",
        value=session_id,
        httponly=True,
        max_age=SESSION_EXPIRE_HOURS * 3600,
        samesite="lax"
    )
    logger.info(f"New user signed up: {email}")
    return {"success": True, "user": {"email": email, "name": body.name}}

@app.get("/api/logout")
async def api_logout(request: Request, response: Response):
    session_id = request.cookies.get("session_id")
    if session_id and session_id in sessions:
        del sessions[session_id]
    response.delete_cookie("session_id")
    return RedirectResponse(url="/login", status_code=302)

@app.get("/api/me")
async def api_me(request: Request):
    session_id = request.cookies.get("session_id")
    user_id = validate_session(session_id)
    if not user_id or user_id not in users_db:
        raise HTTPException(401, detail="Not authenticated")
    return {"email": user_id, "name": users_db[user_id]["name"]}

@app.get("/auth/google")
async def google_auth():
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(500, detail="Google OAuth not configured")
    
    google_auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth?"
        f"client_id={GOOGLE_CLIENT_ID}&"
        f"redirect_uri={GOOGLE_REDIRECT_URI}&"
        "response_type=code&"
        "scope=openid%20email%20profile&"
        "access_type=offline"
    )
    return RedirectResponse(url=google_auth_url)

@app.get("/auth/google/callback")
async def google_callback(request: Request, response: Response, code: str = None, error: str = None):
    if error:
        return RedirectResponse(url=f"/login?error={error}")
    
    if not code:
        return RedirectResponse(url="/login?error=no_code")
    
    try:
        async with httpx.AsyncClient() as client:
            token_response = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "code": code,
                    "client_id": GOOGLE_CLIENT_ID,
                    "client_secret": GOOGLE_CLIENT_SECRET,
                    "redirect_uri": GOOGLE_REDIRECT_URI,
                    "grant_type": "authorization_code"
                }
            )
            
            if token_response.status_code != 200:
                logger.error(f"Google token error: {token_response.text}")
                return RedirectResponse(url="/login?error=token_failed")
            
            tokens = token_response.json()
            access_token = tokens.get("access_token")
            
            user_response = await client.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            
            if user_response.status_code != 200:
                return RedirectResponse(url="/login?error=userinfo_failed")
            
            user_info = user_response.json()
            email = user_info.get("email", "").lower()
            name = user_info.get("name", email.split("@")[0])
            
            if email not in users_db:
                users_db[email] = {
                    "name": name,
                    "password": "",  # No password for OAuth users
                    "google_id": user_info.get("id"),
                    "created": time.time()
                }
                save_users()
                logger.info(f"New Google user: {email}")
            
            session_id = create_session(email)
            redirect = RedirectResponse(url="/app", status_code=302)
            redirect.set_cookie(
                key="session_id",
                value=session_id,
                httponly=True,
                max_age=SESSION_EXPIRE_HOURS * 3600,
                samesite="lax"
            )
            logger.info(f"Google login: {email}")
            return redirect
            
    except Exception as e:
        logger.error(f"Google OAuth error: {e}")
        return RedirectResponse(url="/login?error=oauth_failed")

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(content=b"", media_type="image/x-icon")

@app.get("/health")
async def health():
    return {"status": "ok", "version": "2.1.0", "providers": debate_client.get_available_providers(), "tavily": tavily_search.is_available()}

@app.get("/providers")
async def get_providers(request: Request):
    session_id = request.cookies.get("session_id")
    if not validate_session(session_id):
        raise HTTPException(401, detail="Not authenticated")
    available = debate_client.get_available_providers()
    result = {name: {"available": name in available, "display_name": config["display_name"], "model": config["model"], "color": config["color"]} for name, config in PROVIDER_CONFIG.items()}
    result["tavily"] = {"available": tavily_search.is_available(), "display_name": "Tavily", "color": "#22c55e"}
    return result

@app.get("/stats")
async def stats(request: Request):
    session_id = request.cookies.get("session_id")
    if not validate_session(session_id):
        raise HTTPException(401, detail="Not authenticated")
    return {"providers": {name: {**debate_client.stats[name], "color": PROVIDER_CONFIG[name]["color"]} for name in PROVIDER_CONFIG}}

@app.post("/debate", response_model=DebateResponse)
async def debate(request: Request, body: AskRequest):
    session_id = request.cookies.get("session_id")
    if not validate_session(session_id):
        raise HTTPException(401, detail="Not authenticated")
    
    client_id = get_client_id(request)
    if not rate_limiter.is_allowed(client_id):
        raise HTTPException(429, detail=f"Rate limited. Try again in {RATE_LIMIT_WINDOW}s")
    
    request_id = secrets.token_hex(6)
    logger.info(f"[{request_id}] Query: {body.question[:40]}...")
    start = time.time()
    
    research = None
    context = None
    total_tokens = 0
    
    if body.use_research and tavily_search.is_available():
        research = await tavily_search.search(body.question)
        if research.get("answer"):
            context = f"{research['answer'][:400]}"
    
    responses = await debate_client.debate(body.question, body.providers, context)
    
    for r in responses.values():
        if r.get("success"):
            total_tokens += r.get("tokens", 0)
    
    final = debate_client.synthesize_answer(responses, research)
    processing_time = (time.time() - start) * 1000
    
    logger.info(f"[{request_id}] Done in {processing_time:.0f}ms, {total_tokens} tokens")
    
    return DebateResponse(request_id=request_id, question=body.question, research=research, responses=responses, 
                          final_answer=final, processing_time_ms=round(processing_time, 2), providers_used=list(responses.keys()), tokens_used=total_tokens)

if __name__ == "__main__":
    import uvicorn
    print("\n" + "=" * 50)
    print("AI Debate Arena v2.1.0 (Secured)")
    print("=" * 50)
    print("\nhttp://localhost:8000\n")
    print("Security:")
    print("  - Authentication: Enabled")
    print("  - Rate limiting: 30 req/min")
    print("  - Input sanitization: Active")
    print("  - Security headers: Enabled\n")
    print("Providers:")
    keys = {"groq": GROQ_API_KEY, "openrouter": OPENROUTER_API_KEY, "chutes": CHUTES_API_KEY, "bytez": BYTEZ_API_KEY}
    for p in PROVIDER_CONFIG:
        print(f"  {'[OK]' if keys.get(p) else '[--]'} {p}")
    print(f"\nTavily: {'[OK]' if TAVILY_API_KEY else '[--]'}")
    print()
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)