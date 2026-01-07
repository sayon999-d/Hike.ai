import os
import time
import json
import uuid
import re
import random
import logging
import asyncio
import threading
import hashlib
import secrets
import httpx
import redis
import bcrypt
import requests
import numpy as np
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from collections import deque
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response, Cookie, WebSocket, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Request, Response, Cookie, WebSocket, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, EmailStr
from dotenv import load_dotenv, find_dotenv
from jose import jwt, JWTError
from authlib.integrations.starlette_client import OAuth
from starlette.middleware.sessions import SessionMiddleware
from starlette.config import Config
from sentence_transformers import SentenceTransformer

from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Float
from sqlalchemy.orm import sessionmaker, declarative_base, Session

try:
    from confluent_kafka import Consumer, Producer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

try:
    import pathway as pw
    PATHWAY_AVAILABLE = True
except ImportError:
    PATHWAY_AVAILABLE = False

try:
    import google.generativeai as genai
    GENAI_AVAILABLE = True
except ImportError:
    GENAI_AVAILABLE = False

try:
    from tavily import TavilyClient
    TAVILY_AVAILABLE = True
except ImportError:
    TAVILY_AVAILABLE = False

try:
    from bytez import Bytez
    BYTEZ_AVAILABLE = True
except ImportError:
    BYTEZ_AVAILABLE = False

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

load_dotenv(find_dotenv())

SECRET_KEY = os.getenv("SECRET_KEY", "default-insecure-secret-key-do-not-use-in-prod")
SESSION_SECRET = os.getenv("SESSION_SECRET", "default-session-secret")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///unified_ai.db")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:9092")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "60"))
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
CHUTES_API_KEY = os.getenv("CHUTES_API_KEY", "")
BYTEZ_API_KEY = os.getenv("BYTEZ_API_KEY", "")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", 150))
MAX_QUERY_LENGTH = int(os.getenv("MAX_QUERY_LENGTH", 2000))

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/auth/google/callback")

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("UnifiedAI")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

try:
    redis_db = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    redis_db.ping()
    logger.info("Redis connected")
except Exception as e:
    logger.warning(f"Redis connection failed: {e}. Some features may be degraded.")
    redis_db = None  # Handle gracefully in code

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, index=True) # Used for email in some contexts
    email = Column(String, unique=True, index=True, nullable=True) 
    password_hash = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    full_name = Column(String, nullable=True)
    google_id = Column(String, nullable=True)

class ConversationLog(Base):
    __tablename__ = "conversation_logs"
    id = Column(Integer, primary_key=True)
    user = Column(String, index=True)
    agent = Column(String)
    message = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

class MemorySummary(Base):
    __tablename__ = "memory_summaries"
    id = Column(Integer, primary_key=True)
    user = Column(String)
    summary = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

class DecisionRecord(Base):
    __tablename__ = "decisions"
    id = Column(String, primary_key=True) # UUID
    user_id = Column(String, index=True)
    context = Column(Text)
    emotion = Column(String)
    action = Column(String)
    domain = Column(String)
    regret = Column(Float)
    final_regret = Column(Float, nullable=True)
    reflection = Column(Text, nullable=True)
    timestamp = Column(Float)

Base.metadata.create_all(engine)

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7
SESSION_EXPIRE_HOURS = 24

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
    except Exception:
        return False

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire, "type": "access"})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def create_refresh_token(user: str):
    jti = str(uuid.uuid4())
    payload = {
        "sub": user,
        "jti": jti,
        "type": "refresh",
        "exp": datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    if redis_db:
        redis_db.setex(f"refresh:{jti}", REFRESH_TOKEN_EXPIRE_DAYS * 86400, user)
    return token

def get_user_from_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("type") != "access":
            return None
        return payload.get("sub")
    except JWTError:
        return None

sessions: Dict[str, Dict] = {}

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

from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware

# Rate limiting
from collections import defaultdict

class SecurityMiddleware:
    """Adds security headers to all responses."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        
        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                headers = dict(message.get("headers", []))
                # Security Headers
                headers[b"x-content-type-options"] = b"nosniff"
                headers[b"x-frame-options"] = b"DENY"
                headers[b"x-xss-protection"] = b"1; mode=block"
                headers[b"strict-transport-security"] = b"max-age=31536000; includeSubDomains"
                headers[b"content-security-policy"] = b"default-src 'self' 'unsafe-md-eval' 'unsafe-inline' https: blob: data:;"
                headers[b"referrer-policy"] = b"strict-origin-when-cross-origin"
                
                message["headers"] = list(headers.items())
            await send(message)

        await self.app(scope, receive, send_wrapper)

class TokenOptimizer:
    """
    Optimizes context window usage by:
    1. Analyzing token count (approximate)
    2. Truncating non-essential history
    3. Summarizing long text if needed
    """
    @staticmethod
    def count_tokens(text: str) -> int:
        # Approximate 4 chars per token for English
        return len(text) // 4

    @staticmethod
    def optimize_context(context: str, max_tokens: int = 1500) -> str:
        """Truncates context to a safe token limit while preserving the end (most recent)."""
        if not context: return ""
        
        current_tokens = TokenOptimizer.count_tokens(context)
        if current_tokens <= max_tokens:
            return context
            
        # Keep the last max_tokens worth of text (approx)
        chars_to_keep = max_tokens * 4
        truncated = context[-chars_to_keep:]
        
        # Try to cut at a discrete line or sentence start to be cleaner
        first_newline = truncated.find('\n')
        if first_newline != -1 and first_newline < 100:
            truncated = truncated[first_newline+1:]
            
        return f"...(earlier context summarized)...\n{truncated}"

class ProviderRateLimiter:
    """
    Advanced rate limiter using Token Bucket algorithm.
    Allows bursts but enforces long-term rate limits.
    """
    def __init__(self):
        # Configuration: (max_tokens, refill_rate_per_sec)
        self.configs = {
            "gemini": (30, 0.5),      # 30 burst, 30 RPM
            "groq": (20, 0.33),       # 20 burst, 20 RPM
            "openrouter": (50, 1.0),  # 50 burst, 60 RPM
            "tavily": (5, 0.1),       # 5 burst, 6 requests per minute (strict)
            "newsapi": (5, 0.05),     # 5 burst, 3 requests per minute (very strict)
            "default": (10, 0.2)
        }
        self.buckets = defaultdict(lambda: {"tokens": 10.0, "last_update": time.time()})
        self._lock = asyncio.Lock()
        
    async def wait_if_needed(self, provider: str):
        config = self.configs.get(provider, self.configs["default"])
        max_tokens, refill_rate = config
        
        async with self._lock:
            bucket = self.configs.get(provider)
            # If explicit config exists, use it, else generic bucket
            key = provider if provider in self.configs else "default"
            bucket = self.buckets[key]
            
            now = time.time()
            elapsed = now - bucket["last_update"]
            
            # Refill tokens
            new_tokens = elapsed * refill_rate
            bucket["tokens"] = min(max_tokens, bucket["tokens"] + new_tokens)
            bucket["last_update"] = now
            
            # Consume token
            if bucket["tokens"] >= 1:
                bucket["tokens"] -= 1
                return # Allowed
            
            # If not allowed, calculating wait time
            required = 1.0 - bucket["tokens"]
            wait_time = required / refill_rate
            
        if wait_time > 0:
            logger.warning(f"Rate limit hit for {provider}, waiting {wait_time:.2f}s")
            await asyncio.sleep(wait_time)
            
            # After waiting, recurse to ensure we consume the token properly (and update timestamp)
            await self.wait_if_needed(provider)

rate_limiter = ProviderRateLimiter()

# Simple user IP-based rate limiting for API endpoints
class UserRateLimiter:
    def __init__(self, requests_per_minute: int = 60):
        self.rate = requests_per_minute
        self.history = defaultdict(list)
        
    def check_rate_limit(self, client_ip: str):
        now = time.time()
        # Clean up old history
        self.history[client_ip] = [t for t in self.history[client_ip] if now - t < 60]
        
        if len(self.history[client_ip]) >= self.rate:
            raise HTTPException(status_code=429, detail="Too many requests. Please slow down.")
            
        self.history[client_ip].append(now)

user_limiter = UserRateLimiter(requests_per_minute=50)

def verify_rate_limit(request: Request):
    """Dependency for API routes"""
    client_ip = request.client.host
    user_limiter.check_rate_limit(client_ip)

class GeminiOrchestrator:
    """
    Central orchestrator using Gemini API to:
    1. Understand user context and intent
    2. Generate instructions for other AI systems
    3. Coordinate responses from all modules
    
    Architecture:
    - Research/Real-time Data: Tavily API
    - News: NewsAPI
    - Debate: All models EXCEPT Gemini (Groq, OpenRouter, Bytez, Chutes)
    - Empathy: User-selected model
    - Regret AI: All available models
    - Orchestration: Gemini API (this class)
    """
    
    def __init__(self):
        self.gemini_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        if self.gemini_key and GENAI_AVAILABLE:
            genai.configure(api_key=self.gemini_key)
            self.model = genai.GenerativeModel("gemini-1.5-flash")
            logger.info("Gemini Orchestrator initialized")
        else:
            self.model = None
            logger.warning("Gemini not available - orchestrator will use fallback mode")
    
    def analyze_context(self, user_message: str, conversation_history: List[Dict] = None) -> Dict:
        """
        Analyze user message and determine what each AI system should do.
        Returns structured instructions for all systems.
        """
        if not self.model:
            return self._fallback_analysis(user_message)
        
        history_context = ""
        if conversation_history:
            history_context = "\n".join([
                f"User: {h.get('user', '')}\nAI: {h.get('ai', '')}" 
                for h in conversation_history[-3:]  # Last 3 exchanges
            ])
        
        analysis_prompt = f"""You are an AI orchestrator. Analyze this user message and determine what each AI module should do.

User Message: "{user_message}"

{f"Recent conversation context:{chr(10)}{history_context}" if history_context else ""}

Respond in this exact JSON format (no markdown, just JSON):
{{
    "intent": "one of: question, decision, emotional_support, information_seeking, advice, debate, casual",
    "needs_research": true/false,
    "research_query": "optimized search query if needs_research is true, else empty string",
    "needs_news": true/false,
    "news_keywords": "relevant news keywords if needs_news is true, else empty string",
    "debate_question": "question for debate models if this is a debatable topic, else empty string",
    "debate_providers": ["list of providers to use: groq, openrouter, bytez, chutes - never include gemini"],
    "emotion_detected": "sadness/anger/fear/joy/neutral/seeking_advice",
    "empathy_instruction": "specific instruction for empathy AI on how to respond",
    "needs_regret_analysis": true/false,
    "regret_context": "context for regret AI if this involves a decision, else empty string",
    "final_response_instruction": "how to combine all responses into a cohesive answer"
}}"""
        
        try:
            response = self.model.generate_content(analysis_prompt)
            text = response.text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            text = text.strip()
            
            return json.loads(text)
        except Exception as e:
            logger.error(f"Gemini orchestration error: {e}")
            return self._fallback_analysis(user_message)
    
    def _fallback_analysis(self, message: str) -> Dict:
        msg_lower = message.lower()
        
        is_question = "?" in message or any(w in msg_lower for w in ["what", "how", "why", "when", "where", "who"])
        is_decision = any(w in msg_lower for w in ["should i", "decide", "choice", "option", "better"])
        is_emotional = any(w in msg_lower for w in ["feel", "sad", "happy", "angry", "worried", "stress"])
        
        return {
            "intent": "decision" if is_decision else ("emotional_support" if is_emotional else ("question" if is_question else "casual")),
            "needs_research": is_question and len(message) > 20,
            "research_query": message[:200] if is_question else "",
            "needs_news": any(w in msg_lower for w in ["news", "latest", "today", "current", "recent"]),
            "news_keywords": message[:50] if "news" in msg_lower else "",
            "debate_question": message if is_question or is_decision else "",
            "debate_providers": ["groq", "openrouter"],  # Default providers (no Gemini)
            "emotion_detected": "seeking_advice" if is_decision else ("neutral" if not is_emotional else "neutral"),
            "empathy_instruction": "Respond with understanding and support",
            "needs_regret_analysis": is_decision,
            "regret_context": message if is_decision else "",
            "final_response_instruction": "Combine insights from all AI systems into a helpful response"
        }
    
    def synthesize_response(self, 
                           user_message: str,
                           orchestration: Dict,
                           research_data: Dict = None,
                           news_data: List = None,
                           debate_data: Dict = None,
                           empathy_response: str = None,
                           regret_data: Dict = None) -> str:
        """
        Use Gemini to synthesize all AI responses into a coherent final response.
        """
        if not self.model:
            return self._fallback_synthesis(empathy_response, debate_data, regret_data, research_data)
        
        synthesis_prompt = f"""You are synthesizing responses from multiple AI systems into one cohesive answer.

User's original message: "{user_message}"
User's intent: {orchestration.get('intent', 'unknown')}

Available data from AI systems:
{f"Research findings: {json.dumps(research_data)}" if research_data else "No research data"}
{f"Relevant news: {json.dumps(news_data[:3]) if news_data else 'No news data'}" if news_data else ""}
{f"Debate perspectives: {debate_data.get('final_answer', 'No debate data')}" if debate_data else "No debate data"}
{f"Empathetic response: {empathy_response}" if empathy_response else "No empathy response"}
{f"Regret analysis: Action suggested: {regret_data.get('action')}, Regret score: {regret_data.get('regret')}" if regret_data else "No regret analysis"}

Instruction: {orchestration.get('final_response_instruction', 'Create a helpful, comprehensive response')}

Create a well-structured, helpful response that:
1. Directly addresses the user's message
2. Incorporates relevant insights from the AI systems
3. Is warm and conversational
4. Provides actionable advice if applicable

Response:"""

        try:
            response = self.model.generate_content(synthesis_prompt)
            return response.text.strip()
        except Exception as e:
            logger.error(f"Gemini synthesis error: {e}")
            return self._fallback_synthesis(empathy_response, debate_data, regret_data, research_data)
    
    def _fallback_synthesis(self, empathy: str, debate: Dict, regret: Dict, research: Dict) -> str:
        parts = []
        if empathy:
            parts.append(empathy)
        if debate and debate.get("final_answer"):
            parts.append(f"\n\n**Additional Perspectives:**\n{debate['final_answer'][:500]}")
        if regret and regret.get("action"):
            parts.append(f"\n\n**Suggested Action:** {regret['action']} (Regret risk: {regret.get('regret', 0):.1%})")
        if research and research.get("answer"):
            parts.append(f"\n\n**Research Context:** {research['answer'][:300]}")
        
        return "\n".join(parts) if parts else "I'm here to help. Could you tell me more?"

orchestrator = GeminiOrchestrator()

class NewsSystem:
    def __init__(self):
        self.news_index = []
        self.model = SentenceTransformer(EMBEDDING_MODEL)
        self.seen_urls = set()

    def fetch_news(self):
        if not NEWSAPI_KEY:
            return []
        try:
            url = "https://newsapi.org/v2/top-headlines"
            params = {"apiKey": NEWSAPI_KEY, "country": "us", "pageSize": 20}
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json().get("articles", [])
        except Exception as e:
            logger.error(f"NewsAPI error: {e}")
            return []

    def summarize_text(self, text: str) -> str:
        if not text: return ""
        try:
            import google.generativeai as genai
            if not GOOGLE_API_KEY: raise Exception("No API Key")
            genai.configure(api_key=GOOGLE_API_KEY)
            model = genai.GenerativeModel("gemini-1.5-flash")
            response = model.generate_content(f"Summarize in 2-3 sentences: {text[:4000]}")
            return response.text.strip()
        except Exception:
            return '. '.join([s.strip() for s in text.split('.') if s.strip()][:3])

    def process_article(self, article):
        try:
            title = article.get("title", "")
            desc = article.get("description", "") or ""
            url = article.get("url", "")
            fetched_at = article.get("fetched_at", datetime.utcnow().isoformat())
            
            text = f"{title}. {desc}".strip()
            
            if len(self.news_index) % 5 == 0:
                summary = self.summarize_text(text)
            else:
                sentences = [s.strip() for s in text.split('.') if s.strip()]
                summary = '. '.join(sentences[:3])

            if summary:
                embedding = self.model.encode([summary], normalize_embeddings=True)[0]
                self.news_index.append({
                    "text": summary,
                    "url": url,
                    "title": title,
                    "description": desc,
                    "urlToImage": article.get("urlToImage"),
                    "publishedAt": article.get("publishedAt"),
                    "source": article.get("source"),
                    "embedding": embedding.tolist(),
                    "fetched_at": fetched_at,
                    "is_realtime": True,
                    "processed_at": datetime.utcnow().isoformat()
                })
                if len(self.news_index) > 100:
                    self.news_index.pop(0)
                logger.info(f"Indexed news: {title[:30]}...")
        except Exception as e:
            logger.error(f"News processing error: {e}")

    def run_fetcher(self):
        logger.info("Starting News Fetcher")
        while True:
            try:
                articles = self.fetch_news()
                timestamp = datetime.utcnow().isoformat()
                for a in articles:
                    url = a.get("url")
                    if url and url not in self.seen_urls:
                        self.seen_urls.add(url)
                        a['fetched_at'] = timestamp
                        self.process_article(a)
                
                if len(self.seen_urls) > 1000:
                    self.seen_urls = set(list(self.seen_urls)[-500:])
            except Exception as e:
                logger.error(f"Fetcher loop error: {e}")
            time.sleep(FETCH_INTERVAL)

    def start(self):
        threading.Thread(target=self.run_fetcher, daemon=True).start()

news_system = NewsSystem()

class EmpatheticSystem:
    """
    Empathetic AI system that uses the user-selected model.
    Supports: Groq, OpenRouter, Bytez, Chutes (based on user settings)
    """
    def __init__(self):
        self.providers = {
            "groq": {"url": "https://api.groq.com/openai/v1/chat/completions", "model": "llama-3.3-70b-versatile", "key": GROQ_API_KEY},
            "openrouter": {"url": "https://openrouter.ai/api/v1/chat/completions", "model": "deepseek/deepseek-r1:free", "key": OPENROUTER_API_KEY},
            "chutes": {"url": "https://llm.chutes.ai/v1/chat/completions", "model": "chutesai/Mistral-Small-3.1-24B-Instruct-2503", "key": CHUTES_API_KEY},
            "bytez": {"model": "meta-llama/Meta-Llama-3.1-8B", "key": BYTEZ_API_KEY},
        }

    def detect_emotion(self, text: str) -> str:
        t = text.lower()
        if any(w in t for w in ["sad", "empty", "lost", "depressed", "lonely", "crying", "tears", "hopeless", "hurt", "broken"]): return "sadness"
        if any(w in t for w in ["angry", "mad", "furious", "annoyed", "frustrated", "hate", "pissed", "rage"]): return "anger"
        if any(w in t for w in ["fear", "anxious", "scared", "worried", "nervous", "panic", "stress", "overwhelmed"]): return "fear"
        if any(w in t for w in ["happy", "good", "great", "excited", "wonderful", "amazing", "love", "joy", "blessed"]): return "joy"
        if any(w in t for w in ["what should", "how do", "how can", "what can", "advice", "help"]): return "seeking_advice"
        return "neutral"

    def determine_strategy(self, emotion: str) -> str:
        return {
            "sadness": "comfort", "anger": "validation", "fear": "reassurance",
            "joy": "celebration", "neutral": "listening", "seeking_advice": "guidance"
        }.get(emotion, "listening")

    def call_llm(self, prompt: str, selected_model: str = "auto") -> str:
        """
        Call the user-selected model for empathetic responses.
        selected_model: 'auto', 'groq', 'openrouter', 'bytez', 'chutes'
        """
        providers_to_try = [selected_model] if selected_model != "auto" else ["groq", "openrouter", "chutes"]
        
        for provider_name in providers_to_try:
            conf = self.providers.get(provider_name)
            if not conf or not conf.get("key"):
                continue
                
            try:
                if provider_name == "bytez" and BYTEZ_AVAILABLE:
                    sdk = Bytez(conf["key"])
                    model = sdk.model(conf["model"])
                    results = model.run(prompt)
                    if results and hasattr(results, 'output'):
                        return results.output
                    continue
                
                response = requests.post(
                    conf["url"],
                    headers={"Authorization": f"Bearer {conf['key']}", "Content-Type": "application/json"},
                    json={
                        "model": conf["model"],
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 300,
                        "temperature": 0.7
                    },
                    timeout=30
                )
                if response.status_code == 200:
                    data = response.json()
                    content = data['choices'][0]['message']['content']
                    if content:
                        return content
            except Exception as e:
                logger.warning(f"Empathy {provider_name} error: {e}")
                continue
        
        return "I'm having trouble connecting to my thought centers, but I'm here listening."

    def generate_response(self, message: str, emotion: str, strategy: str, selected_model: str = "auto") -> str:
        opt_msg = TokenOptimizer.optimize_context(message, max_chars=1000)
        
        prompt = f"""You are an empathetic AI. 
        User message: "{opt_msg}"
        Detected emotion: {emotion}
        Strategy: {strategy}
        Respond warmly, naturally, and helpfully (2-3 sentences)."""
        
        response = self.call_llm(prompt, selected_model)
        if len(response) < 5 or "trouble connecting" in response:
            fallbacks = {
                "sadness": "I'm so sorry you're going through this. I'm here for you.",
                "anger": "It makes sense that you'd feel that way. Want to tell me more?",
                "joy": "That's amazing! I'm so happy for you.",
            }
            return fallbacks.get(emotion, response)
        return response

    def log_interaction(self, user: str, message: str, emotion: str, response: str):
        db = SessionLocal()
        try:
            db.add(ConversationLog(user=user, agent="empathetic", message=message))
            db.add(ConversationLog(user=user, agent="response", message=response))
            if redis_db:
                redis_db.rpush(f"timeline:{user}", json.dumps({"emotion": emotion, "time": datetime.utcnow().isoformat()}))
                redis_db.hincrby(f"heatmap:{user}", emotion, 1)
            db.commit()
        except Exception as e:
            logger.error(f"Logging error: {e}")
        finally:
            db.close()

empathetic_system = EmpatheticSystem()

class RegretSystem:
    def __init__(self):
        self.actions = {
            "Apply for a new job": "career", "Switch current job": "career", "Ask for a promotion": "career",
            "Save money": "finance", "Invest in stocks": "finance", "Pay off debt": "finance",
            "Start exercising": "health", "Improve diet": "health",
            "Repair relationship": "relationships", "End relationship": "relationships"
        }
        self.ollama_url = "http://localhost:11434"
        self.encoder = SentenceTransformer("all-MiniLM-L6-v2")

    def call_ollama(self, prompt: str) -> Optional[str]:
        try:
            res = requests.post(f"{self.ollama_url}/api/generate", 
                              json={"model": "mistral", "prompt": prompt, "stream": False}, timeout=5)
            if res.status_code == 200: return res.json().get("response")
        except: return None

    def predict_outcome(self, context: str, action: str) -> float:
        prompt = f"Context: {context}. Action: {action}. Rate outcome -10 to +10. Return ONLY number."
        resp = self.call_ollama(prompt)
        if resp:
            try:
                import re
                nums = re.findall(r'-?\d+\.?\d*', resp)
                if nums: return max(-10, min(10, float(nums[0])))
            except: pass
        return random.uniform(-5, 5)

    def make_decision(self, user_id: str, context: str, emotion: str) -> Dict:
        possible_actions = list(self.actions.keys())
        chosen_action = random.choice(possible_actions)
        
        score = self.predict_outcome(context, chosen_action)
        regret = max(0, 10 - score) # Simple regret metric

        decision_id = str(uuid.uuid4())
        record = DecisionRecord(
            id=decision_id, user_id=user_id, context=context, emotion=emotion,
            action=chosen_action, domain=self.actions[chosen_action], 
            regret=regret, timestamp=time.time()
        )
        
        db = SessionLocal()
        try:
            db.add(record)
            db.commit()
        finally:
            db.close()
            
        return {
            "decision_id": decision_id, "action": chosen_action, 
            "domain": self.actions[chosen_action], "regret": regret
        }

regret_system = RegretSystem()

class DebateSystem:
    def __init__(self):
        self.providers = {
            "groq": {"url": "https://api.groq.com/openai/v1/chat/completions", "model": "llama-3.3-70b-versatile", "key": GROQ_API_KEY},
            "openrouter": {"url": "https://openrouter.ai/api/v1/chat/completions", "model": "deepseek/deepseek-r1:free", "key": OPENROUTER_API_KEY},
            "chutes": {"url": "https://api.chutes.ai/v1/chat/completions", "model": "chutesai/Mistral-Small-3.1-24B-Instruct-2503", "key": CHUTES_API_KEY},
            "bytez": {"url": "", "model": "meta-llama/Meta-Llama-3.1-8B", "key": BYTEZ_API_KEY},
        }
        self.tavily = TavilyClient(api_key=TAVILY_API_KEY) if TAVILY_AVAILABLE and TAVILY_API_KEY else None

    async def call_provider(self, name: str, question: str, context: str = "") -> Dict:
        conf = self.providers.get(name)
        if not conf or not conf["key"]: return {"success": False, "error": "Not configured"}
        
        await rate_limiter.wait_if_needed(name)
        
        optimized_context = TokenOptimizer.optimize_context(context, max_chars=3000)
        msg_content = f"Context: {optimized_context}\n\nQuestion: {question}" if optimized_context else question
        
        if name == "chutes" and AIOHTTP_AVAILABLE:
            try:
                headers = {
                    "Authorization": "Bearer " + conf["key"],
                    "Content-Type": "application/json"
                }
                body = {
                    "model": "chutesai/Mistral-Small-3.1-24B-Instruct-2503", 
                    "messages": [{"role": "user", "content": msg_content}],
                    "stream": True,
                    "max_tokens": 1024,
                    "temperature": 0.7
                }
                
                full_response = ""
                async with aiohttp.ClientSession() as session:
                    async with session.post("https://llm.chutes.ai/v1/chat/completions", headers=headers, json=body) as response:
                        if response.status != 200:
                            return {"success": False, "error": f"Status {response.status}"}
                            
                        async for line in response.content:
                            line = line.decode("utf-8").strip()
                            if line.startswith("data: "):
                                data = line[6:]
                                if data == "[DONE]": break
                                try:
                                    chunk = json.loads(data)
                                    if chunk.get("choices") and chunk["choices"][0].get("delta", {}).get("content"):
                                        full_response += chunk["choices"][0]["delta"]["content"]
                                except Exception: pass
                                
                return {"success": True, "response": full_response, "model": body["model"]}
            except Exception as e:
                logger.error(f"Chutes error: {e}")
                return {"success": False, "error": str(e)}

        if name == "bytez" and BYTEZ_AVAILABLE:
            try:
                sdk = Bytez(conf["key"])
                model = sdk.model(conf["model"])
                
                results = await asyncio.to_thread(model.run, msg_content)
                
                if results and hasattr(results, 'output'):
                     return {"success": True, "response": results.output, "model": conf["model"]}
                elif results and hasattr(results, 'error') and results.error:
                     return {"success": False, "error": str(results.error)}
                else:
                     return {"success": False, "error": "Unknown Bytez error"}
            except Exception as e:
                logger.error(f"Bytez error: {e}")
                return {"success": False, "error": str(e)}

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                res = await client.post(
                    conf["url"],
                    headers={"Authorization": f"Bearer {conf['key']}"},
                    json={
                        "model": conf["model"],
                        "messages": [{"role": "user", "content": msg_content}],
                        "max_tokens": MAX_OUTPUT_TOKENS
                    }
                )
                if res.status_code == 200:
                    data = res.json()
                    content = data['choices'][0]['message']['content']
                    return {"success": True, "response": content, "model": conf["model"]}
                return {"success": False, "error": f"Status {res.status_code}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def conduct_debate(self, question: str, providers: List[str], use_research: bool = False):
        research_context = ""
        research_data = None
        
        if use_research and self.tavily:
            try:
                res = self.tavily.search(query=question, max_results=3, include_answer=True)
                research_context = res.get("answer", "")
                research_data = {"answer": res.get("answer"), "sources": res.get("results")}
            except Exception as e:
                logger.error(f"Research failed: {e}")

        tasks = [self.call_provider(p, question, research_context) for p in providers if p in self.providers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        response_map = {}
        valid_responses = []
        
        for p, res in zip([p for p in providers if p in self.providers], results):
            if isinstance(res, dict):
                response_map[p] = res
                if res.get("success"): valid_responses.append(f"{p}: {res['response']}")
            else:
                response_map[p] = {"success": False, "error": str(res)}

        final_answer = "\n\n".join(valid_responses) if valid_responses else "No successful responses."
        
        return {
            "responses": response_map,
            "research": research_data,
            "final_answer": final_answer,

            "request_id": secrets.token_hex(4)
        }

debate_system = DebateSystem()

app = FastAPI(title="Unified AI System", description="News, Empathy, Debate, and Regret - All in one.")
templates = Jinja2Templates(directory="templates")
ALLOWED_ORIGINS = [
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    BACKEND_URL
]
ALLOWED_ORIGINS = list(set(filter(None, ALLOWED_ORIGINS)))

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-CSRF-Token"],
)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["localhost", "127.0.0.1", "0.0.0.0"])
app.add_middleware(SecurityMiddleware)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)

LANDING_HTML = """
<!DOCTYPE html>
<html>
<head><title>Unified AI</title><style>body{font-family:sans-serif;background:#111;color:#fff;display:flex;justify-content:center;align-items:center;height:100vh}a{color:#fff;text-decoration:none;border:1px solid #333;padding:20px;border-radius:10px;margin:10px;display:block;width:200px;text-align:center;transition:0.3s}a:hover{background:#222;border-color:#555}</style></head>
<body>
<div>
    <h1>Unified AI System</h1>
    <div style="display:flex;flex-wrap:wrap">
        <a href="/news"> News Flow</a>
        <a href="/chat"> Empathetic Chat</a>
        <a href="/debate"> Debate Arena</a>
        <a href="/regret"> Regret AI</a>
    </div>
</div>
</body>
</html>
"""

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(content=b"", media_type="image/x-icon")

@app.get("/", response_class=HTMLResponse)
def landing(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/login", response_class=HTMLResponse)
def login_redirect(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/signup", response_class=HTMLResponse)
def signup_redirect(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/news", response_class=HTMLResponse)
def news_ui(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/debate", response_class=HTMLResponse)
def debate_ui(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/regret", response_class=HTMLResponse)
def regret_ui(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/chat", response_class=HTMLResponse)
def chat_ui(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
config = Config(environ=os.environ)
oauth = OAuth(config)
oauth.register(
    name="google",
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)

class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=128)

class SignupRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=128)

@app.get("/api/profile")
def get_profile(request: Request, db: Session = Depends(get_db)):
    user_id = validate_session(request.cookies.get("session_id"))
    if not user_id: 
        raise HTTPException(401, detail="Not authenticated")
    user = db.query(User).filter(User.email == user_id).first()
    return {"email": user_id, "name": user.full_name if user else "Unknown"}

@app.post("/api/login")
def api_login(body: LoginRequest, response: Response, db: Session = Depends(get_db), _ = Depends(verify_rate_limit)):
    email = body.email.lower().strip()
    user = db.query(User).filter(User.email == email).first()
    if not user or not verify_password(body.password, user.password_hash):
        raise HTTPException(401, detail="Invalid credentials")
    
    session_id = create_session(email)
    response.set_cookie(key="session_id", value=session_id, httponly=True, max_age=SESSION_EXPIRE_HOURS * 3600, samesite="lax")
    return {"message": "Login successful"}

@app.post("/api/signup")
def api_signup(body: SignupRequest, response: Response, db: Session = Depends(get_db), _ = Depends(verify_rate_limit)):
    email = body.email.lower().strip()
    
    existing = db.query(User).filter(User.email == email).first()
    if existing:
        raise HTTPException(400, detail="Email already registered")
    
    user = User(
        email=email,
        full_name=body.name,
        password_hash=hash_password(body.password),
        username=email
    )
    db.add(user)
    db.commit()
    
    session_id = create_session(email)
    response.set_cookie(key="session_id", value=session_id, httponly=True, max_age=SESSION_EXPIRE_HOURS * 3600, samesite="lax")
    return {"message": "Signup successful"}

@app.get("/api/logout")
def api_logout(request: Request, response: Response):
    session_id = request.cookies.get("session_id")
    if session_id and session_id in sessions:
        del sessions[session_id]
    response.delete_cookie("session_id")
    return RedirectResponse("/")

@app.get("/auth/google")
async def google_login(request: Request):
    return await oauth.google.authorize_redirect(request, GOOGLE_REDIRECT_URI)

@app.get("/auth/google/callback")
async def google_callback(request: Request, response: Response, db: Session = Depends(get_db)):
    try:
        token = await oauth.google.authorize_access_token(request)
        user_info = token.get("userinfo")
        if not user_info or not user_info.get("email"):
            return RedirectResponse("/?error=auth_failed")
        
        email = user_info.get("email")
        name = user_info.get("name", "User")
        
        user = db.query(User).filter(User.email == email).first()
        if not user:
            user = User(email=email, full_name=name, password_hash="google_oauth_placeholder", username=email)
            db.add(user)
            db.commit()
            
        session_id = create_session(email)
        resp = RedirectResponse("/")
        resp.set_cookie(key="session_id", value=session_id, httponly=True, max_age=SESSION_EXPIRE_HOURS * 3600, samesite="lax")
        return resp
        
    except Exception as e:
        logger.error(f"OAuth Error: {e}")
        return RedirectResponse(f"/?error=oauth_error&detail={str(e)}")
@app.get("/api/news/latest")
def get_latest_news():
    return news_system.news_index[-20:]

@app.get("/api/news/summary")
def get_news_summary():
    recent = news_system.news_index[-10:]
    txt = " ".join([r['text'] for r in recent])
    return {"summary": news_system.summarize_text(txt), "count": len(recent)}

class ChatMessage(BaseModel):
    message: str
    token: Optional[str] = None
    selected_model: str = "auto"  # User's selected model for empathy
    use_research: bool = True     # Whether to use Tavily for research
    use_debate: bool = True       # Whether to use debate AI
    debate_models: List[str] = ["groq", "openrouter"]  # Models for debate (2-4)
    use_regret: bool = True       # Whether to use regret AI
    regret_models: List[str] = ["groq", "openrouter"]  # Models for regret (2-4)

@app.post("/api/chat")
async def chat_endpoint(data: ChatMessage, db: Session = Depends(get_db), _ = Depends(verify_rate_limit)):
    """
    Orchestrated chat endpoint that coordinates all AI systems:
    - Gemini: Analyzes context and orchestrates other AIs
    - Tavily: Real-time research and data
    - News: Relevant news from NewsAPI
    - Debate: Multi-perspective analysis (Groq, OpenRouter, Bytez, Chutes - no Gemini)
    - Empathy: User-selected model for empathetic response
    - Regret: Decision analysis using selected models
    """
    user = "anonymous"
    message = data.message
    
    orchestration = orchestrator.analyze_context(message)
    logger.info(f"Orchestration: intent={orchestration.get('intent')}, needs_research={orchestration.get('needs_research')}")
    
    research_data = None
    if data.use_research and orchestration.get("needs_research") and TAVILY_AVAILABLE and TAVILY_API_KEY:
        try:
            tavily = TavilyClient(api_key=TAVILY_API_KEY)
            query = orchestration.get("research_query") or message[:200]
            res = tavily.search(query=query, max_results=3, include_answer=True)
            research_data = {"answer": res.get("answer"), "sources": res.get("results", [])}
            logger.info(f"Research completed: {len(research_data.get('sources', []))} sources")
        except Exception as e:
            logger.error(f"Tavily research error: {e}")
    
    news_data = None
    if orchestration.get("needs_news"):
        try:
            news_data = news_system.news_index[:5] if news_system.news_index else None
        except Exception as e:
            logger.error(f"News fetch error: {e}")
    
    debate_data = None
    if data.use_debate and orchestration.get("debate_question"):
        try:
            providers = data.debate_models if data.debate_models else ["groq", "openrouter"]
            providers = [p for p in providers if p not in ["gemini", "google"]]
            if len(providers) < 2:
                providers = ["groq", "openrouter"]
            if providers:
                debate_data = await debate_system.conduct_debate(
                    orchestration.get("debate_question", message),
                    providers,
                    use_research=False  # Research already done above
                )
                logger.info(f"Debate completed with providers: {providers}")
        except Exception as e:
            logger.error(f"Debate error: {e}")
    
    emotion = orchestration.get("emotion_detected") or empathetic_system.detect_emotion(message)
    strategy = empathetic_system.determine_strategy(emotion)
    empathy_response = empathetic_system.generate_response(
        message, emotion, strategy, 
        selected_model=data.selected_model
    )
    
    regret_data = None
    if data.use_regret and orchestration.get("needs_regret_analysis"):
        try:
            regret_data = regret_system.make_decision(user, message, emotion)
            logger.info(f"Regret analysis completed with models: {data.regret_models}")
        except Exception as e:
            logger.error(f"Regret analysis error: {e}")
    
    final_response = orchestrator.synthesize_response(
        user_message=message,
        orchestration=orchestration,
        research_data=research_data,
        news_data=news_data,
        debate_data=debate_data,
        empathy_response=empathy_response,
        regret_data=regret_data
    )
    
    empathetic_system.log_interaction(user, message, emotion, final_response)
    
    return {
        "response": final_response, 
        "details": {
            "emotion": emotion, 
            "strategy": strategy,
            "intent": orchestration.get("intent"),
            "used_research": research_data is not None,
            "used_debate": debate_data is not None,
            "used_regret": regret_data is not None,
            "timestamp": datetime.utcnow().isoformat()
        },
        "orchestration": {
            "research": {"available": research_data is not None, "sources": len(research_data.get("sources", [])) if research_data else 0},
            "debate": {"available": debate_data is not None, "providers": list(debate_data.get("responses", {}).keys()) if debate_data else []},
            "regret": regret_data
        }
    }

@app.websocket("/ws/chat")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            data = await ws.receive_json()
            msg = data.get("message", "")
            selected_model = data.get("selected_model", "auto")
            emotion = empathetic_system.detect_emotion(msg)
            strategy = empathetic_system.determine_strategy(emotion)
            resp = empathetic_system.generate_response(msg, emotion, strategy, selected_model)
            await ws.send_json({"response": resp, "emotion": emotion})
    except Exception:
        pass

@app.post("/api/decide")
def decide_endpoint(data: dict):
    return regret_system.make_decision(data.get("user_id", "anon"), data.get("context", ""), data.get("emotion", "neutral"))

class DebateRequest(BaseModel):
    question: str
    providers: List[str] = ["groq", "openrouter", "bytez", "chutes"]  # No Gemini in debate
    use_research: bool = True

@app.post("/api/debate")
async def debate_endpoint(req: DebateRequest):
    providers = [p for p in req.providers if p not in ["gemini", "google"]]
    return await debate_system.conduct_debate(req.question, providers, req.use_research)

@app.on_event("startup")
def startup_event():
    news_system.start()
    logger.info("Unified AI System Started")

@app.on_event("shutdown")
def shutdown_event():
    logger.info("Unified AI System Shutting Down") 