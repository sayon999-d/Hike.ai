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
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    redis = None
    REDIS_AVAILABLE = False
import bcrypt
import requests
import numpy as np
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from collections import deque
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response, Cookie, WebSocket, Depends
from fastapi.middleware.cors import CORSMiddleware

from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from pydantic import BaseModel, Field, field_validator, EmailStr
from dotenv import load_dotenv, find_dotenv
from jose import jwt, JWTError
from authlib.integrations.starlette_client import OAuth
from starlette.middleware.sessions import SessionMiddleware
from starlette.config import Config
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    SentenceTransformer = None

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

HTML_CONTENT = r"""
<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Hike.ai</title>

  <style>
    :root {
      --bg-body: #ffffff;
      --bg-panel: #f9fafb;
      --bg-card: #ffffff;
      --text-main: #111827;
      --text-muted: #6b7280;
      --border: #e5e7eb;
      --primary: #2563eb;
      --primary-fg: #ffffff;
      --msg-user-bg: #000000;
      --msg-user-text: #ffffff;
    }
    body.dark {
      --bg-body: #0a0a0a;
      --bg-panel: #171717;
      --bg-card: #171717;
      --text-main: #ededed;
      --text-muted: #a3a3a3;
      --border: #262626;
      --primary: #3b82f6;
      --primary-fg: #ffffff;
      --msg-user-bg: #3b82f6;
      --msg-user-text: #ffffff;
    }
    
    * { box-sizing: border-box; }
    body { 
      margin: 0; 
      font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; 
      background: var(--bg-body); 
      color: var(--text-main);
      height: 100vh; 
      overflow: hidden; 
      transition: background 0.3s, color 0.3s;
    }
    
    /* Layout */
    .app { display: none; height: 100%; width: 100%; }
    .app.active { display: flex; }
    
    .sidebar {
      width: 260px;
      background: var(--bg-panel);
      border-right: 1px solid var(--border);
      display: flex;
      flex-direction: column;
      padding: 16px;
      gap: 8px;
    }
    
    .chat {
      flex: 1;
      display: flex;
      flex-direction: column;
      background: var(--bg-body);
      position: relative;
      height: 100%;
    }
    
    /* Navigation */
    .logo { font-size: 18px; font-weight: 700; margin-bottom: 24px; padding: 0 12px; color: var(--text-main); }
    nav a {
      display: block;
      padding: 10px 12px;
      border-radius: 8px;
      text-decoration: none;
      color: var(--text-muted);
      font-size: 14px;
      font-weight: 500;
      transition: all 0.2s;
    }
    nav a:hover, nav a.active {
      background: var(--bg-card);
      color: var(--text-main);
      box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }
    body.dark nav a:hover, body.dark nav a.active {
      background: #262626;
      box-shadow: none;
    }
    
    .sidebar-bottom { margin-top: auto; }
    .settings-sidebar-btn {
        width: 100%;
        padding: 12px;
        background: var(--bg-card);
        border: 1px solid var(--border);
        color: var(--text-main);
        border-radius: 8px;
        cursor: pointer;
        text-align: left;
    }
    
    /* Login */
    .login-page {
      display: flex;
      align-items: center;
      justify-content: center;
      height: 100vh;
      background: var(--bg-body);
    }
    .login-container {
      width: 100%;
      max-width: 400px;
      padding: 40px;
      background: var(--bg-panel);
      border-radius: 16px;
      border: 1px solid var(--border);
      box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    .login-logo { font-size: 24px; font-weight: 700; margin-bottom: 8px; text-align: center; }
    .login-subtitle { text-align: center; color: var(--text-muted); margin-bottom: 32px; font-size: 14px; }
    .form-group { margin-bottom: 16px; }
    .form-label { display: block; margin-bottom: 6px; font-size: 14px; font-weight: 500; }
    .form-input {
      width: 100%;
      padding: 12px;
      border-radius: 8px;
      border: 1px solid var(--border);
      background: var(--bg-body);
      color: var(--text-main);
      font-size: 14px;
    }
    .form-input:focus { outline: 2px solid var(--text-main); border-color: transparent; }
    .login-btn, .google-btn {
      width: 100%;
      padding: 12px;
      border-radius: 8px;
      border: none;
      cursor: pointer;
      font-weight: 600;
      margin-top: 16px;
      font-size: 14px;
    }
    .login-btn { background: var(--text-main); color: var(--bg-body); }
    .google-btn { background: transparent; border: 1px solid var(--border); color: var(--text-main); }
    
    /* Chat Area */
    .chat-header {
      padding: 16px 24px;
      border-bottom: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: center;
      background: var(--bg-body);
    }
    .chat-body {
      flex: 1;
      overflow-y: auto;
      padding: 24px 20%;
      display: flex;
      flex-direction: column;
      gap: 24px;
    }
    @media (max-width: 1024px) {
        .chat-body { padding: 24px 5%; }
    }
    .message {
      max-width: 80%;
      padding: 14px 18px;
      border-radius: 16px;
      line-height: 1.6;
      font-size: 15px;
    }
    .message.user {
      align-self: flex-end;
      background: var(--msg-user-bg);
      color: var(--msg-user-text);
      border-bottom-right-radius: 4px;
    }
    .message.ai {
      align-self: flex-start;
      background: var(--bg-panel);
      color: var(--text-main);
      border: 1px solid var(--border);
      border-bottom-left-radius: 4px;
    }
    .chat-input {
      padding: 24px 20%;
      border-top: 1px solid var(--border);
      display: flex;
      gap: 12px;
      background: var(--bg-body);
      align-items: flex-end;
    }
    @media (max-width: 1024px) {
        .chat-input { padding: 20px 5%; }
    }
    textarea {
      flex: 1;
      padding: 14px;
      border-radius: 12px;
      border: 1px solid var(--border);
      background: var(--bg-panel);
      color: var(--text-main);
      resize: none;
      font-family: inherit;
      min-height: 52px;
      max-height: 200px;
    }
    #sendBtn {
      padding: 0 20px;
      height: 52px;
      background: var(--text-main);
      color: var(--bg-body);
      border: none;
      border-radius: 12px;
      cursor: pointer;
      font-weight: 600;
    }
    
    /* Modules */
    .project-grid, .news-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
      gap: 24px;
      padding: 32px;
      overflow-y: auto;
      height: 100%;
    }
    .project-card, .news-card {
      background: var(--bg-panel);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 20px;
      cursor: pointer;
      transition: all 0.2s;
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }
    .project-card:hover, .news-card:hover {
        transform: translateY(-2px);
        border-color: var(--text-muted);
    }
    .news-image {
      height: 180px;
      background-size: cover;
      background-position: center;
      border-radius: 12px;
      margin-bottom: 16px;
      background-color: var(--border);
    }
    .news-title { font-size: 16px; font-weight: 600; margin-bottom: 8px; line-height: 1.4; color: var(--text-main); }
    .news-summary { font-size: 14px; color: var(--text-muted); display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden; margin-bottom: 12px; }
    
    /* Agent & Metrics */
    .agent-panel {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 16px;
        padding: 12px 20%;
        background: var(--bg-panel);
        border-bottom: 1px solid var(--border);
        font-size: 12px;
    }
    @media (max-width: 1024px) {
        .agent-panel { padding: 12px 5%; }
    }
    .agent { display: flex; justify-content: space-between; margin-bottom: 4px; color: var(--text-main); }
    .bar { height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; }
    .fill { height: 100%; background: var(--text-main); transition: width 0.5s; }

    /* Utilities */
    .hidden { display: none !important; }
    .divider { display: flex; align-items: center; margin: 24px 0; color: var(--text-muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; }
    .divider::before, .divider::after { content: ""; flex: 1; height: 1px; background: var(--border); }
    .divider span { padding: 0 12px; }
    .switch-mode { text-align: center; margin-top: 24px; font-size: 14px; color: var(--text-muted); }
    .switch-mode a { color: var(--text-main); cursor: pointer; font-weight: 600; text-decoration: underline; text-underline-offset: 4px; }
    .error-msg { background: #fee2e2; color: #b91c1c; padding: 12px; border-radius: 8px; display: none; margin-bottom: 16px; font-size: 14px; }
    .error-msg.show { display: block; }
    .password-toggle { position: absolute; right: 12px; top: 12px; cursor: pointer; font-size: 12px; color: var(--text-muted); }
    
    /* Scrollbar */
    ::-webkit-scrollbar { width: 8px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
    
    .theme-toggle-login {
        position: absolute; top: 20px; right: 20px;
        width: 36px; height: 36px;
        border-radius: 8px;
        background: var(--bg-panel);
        border: 1px solid var(--border);
        display: flex; justify-content: center; align-items: center;
        cursor: pointer;
        color: var(--text-main);
    }
    
    /* Settings Modal */
    .settings-overlay {
        position: fixed; top: 0; left: 0; right: 0; bottom: 0;
        background: rgba(0,0,0,0.5);
        display: none; justify-content: center; align-items: center; z-index: 1000;
        backdrop-filter: blur(4px);
    }
    .settings-overlay.active { display: flex; }
    .settings-panel {
        width: 100%; max-width: 500px;
        background: var(--bg-body);
        padding: 24px;
        border-radius: 16px;
        max-height: 85vh;
        overflow-y: auto;
        position: relative;
    }
    .settings-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
    .settings-title { font-size: 18px; font-weight: 700; }
    .settings-close { background: none; border: none; font-size: 24px; cursor: pointer; color: var(--text-main); }
    
    .setting-card {
        padding: 16px;
        border: 1px solid var(--border);
        border-radius: 8px;
        margin-bottom: 12px;
        cursor: pointer;
        display: flex; align-items: center; justify-content: space-between;
    }
    .setting-card.selected { border-color: var(--text-main); background: var(--bg-panel); }
    
  </style>
</head>

<body>

  <div class="login-page" id="loginPage">
    <div class="theme-toggle-login" onclick="toggleDark()">
      <span class="theme-light">☀</span>
      <span class="theme-dark" style="display:none;">☾</span>
    </div>

    <div class="login-container">
      <div class="login-logo">Hike.ai</div>
      <div class="login-subtitle">Thinking-first AI for better decisions</div>

      <div id="errorMessage" class="error-msg"></div>

      <div id="loginForm">
        <div class="form-group">
          <label class="form-label">Email</label>
          <input type="email" id="loginEmail" class="form-input" placeholder="you@example.com">
        </div>

        <div class="form-group">
          <label class="form-label">Password</label>
          <div style="position: relative;">
            <input type="password" id="loginPassword" class="form-input" placeholder="••••••••"
              style="padding-right: 55px;">
            <span class="password-toggle" onclick="togglePassword('loginPassword', this)">Show</span>
          </div>
        </div>

        <button class="login-btn" id="loginBtn" onclick="handleLogin()">Sign In</button>

        <div class="divider">
          <span>or</span>
        </div>

        <button class="google-btn" onclick="window.location.href='/auth/google'">
          <svg width="18" height="18" viewBox="0 0 18 18">
            <path fill="#4285F4"
              d="M17.64 9.2c0-.637-.057-1.251-.164-1.84H9v3.481h4.844c-.209 1.125-.843 2.078-1.796 2.717v2.258h2.908c1.702-1.567 2.684-3.875 2.684-6.615z" />
            <path fill="#34A853"
              d="M9 18c2.43 0 4.467-.806 5.956-2.184l-2.908-2.258c-.806.54-1.837.86-3.048.86-2.344 0-4.328-1.584-5.036-3.711H.957v2.332C2.438 15.983 5.482 18 9 18z" />
            <path fill="#FBBC05"
              d="M3.964 10.707c-.18-.54-.282-1.117-.282-1.707s.102-1.167.282-1.707V4.96H.957C.347 6.175 0 7.55 0 9s.348 2.825.957 4.04l3.007-2.333z" />
            <path fill="#EA4335"
              d="M9 3.58c1.321 0 2.508.454 3.44 1.345l2.582-2.58C13.463.891 11.426 0 9 0 5.482 0 2.438 2.017.957 4.958L3.964 7.29C4.672 5.163 6.656 3.58 9 3.58z" />
          </svg>
          Continue with Google
        </button>

        <div class="switch-mode">
          Don't have an account? <a onclick="switchToSignup()">Sign up</a>
        </div>
      </div>

      <div id="signupForm" class="hidden">
        <div class="form-group">
          <label class="form-label">Full Name</label>
          <input type="text" id="signupName" class="form-input" placeholder="John Doe">
        </div>

        <div class="form-group">
          <label class="form-label">Email</label>
          <input type="email" id="signupEmail" class="form-input" placeholder="you@example.com">
        </div>

        <div class="form-group">
          <label class="form-label">Password</label>
          <div style="position: relative;">
            <input type="password" id="signupPassword" class="form-input" placeholder="••••••••"
              style="padding-right: 55px;">
            <span class="password-toggle" onclick="togglePassword('signupPassword', this)">Show</span>
          </div>
        </div>

        <button class="login-btn" id="signupBtn" onclick="handleSignup()">Create Account</button>

        <div class="divider">
          <span>or</span>
        </div>

        <button class="google-btn" onclick="window.location.href='/auth/google'">
          <svg width="18" height="18" viewBox="0 0 18 18">
            <path fill="#4285F4"
              d="M17.64 9.2c0-.637-.057-1.251-.164-1.84H9v3.481h4.844c-.209 1.125-.843 2.078-1.796 2.717v2.258h2.908c1.702-1.567 2.684-3.875 2.684-6.615z" />
            <path fill="#34A853"
              d="M9 18c2.43 0 4.467-.806 5.956-2.184l-2.908-2.258c-.806.54-1.837.86-3.048.86-2.344 0-4.328-1.584-5.036-3.711H.957v2.332C2.438 15.983 5.482 18 9 18z" />
            <path fill="#FBBC05"
              d="M3.964 10.707c-.18-.54-.282-1.117-.282-1.707s.102-1.167.282-1.707V4.96H.957C.347 6.175 0 7.55 0 9s.348 2.825.957 4.04l3.007-2.333z" />
            <path fill="#EA4335"
              d="M9 3.58c1.321 0 2.508.454 3.44 1.345l2.582-2.58C13.463.891 11.426 0 9 0 5.482 0 2.438 2.017.957 4.958L3.964 7.29C4.672 5.163 6.656 3.58 9 3.58z" />
          </svg>
          Sign up with Google
        </button>

        <div class="switch-mode">
          Already have an account? <a onclick="switchToLogin()">Sign in</a>
        </div>
      </div>
    </div>
  </div>

  <div class="app" id="mainApp">
    <aside class="sidebar">
      <div class="logo">Hike.ai</div>
      <nav>
        <a class="active" id="nav-chat" onclick="showSection('chat')">AI Chat</a>
        <a id="nav-projects" onclick="showSection('projects')">Projects</a>
        <a id="nav-news" onclick="showSection('news')">News</a>
        <a id="nav-history" onclick="showSection('history')">History</a>
        <a onclick="window.location.href='/api/logout'">Log Out</a>
      </nav>
      <div class="sidebar-bottom">
        <button class="settings-sidebar-btn" onclick="openSettings()">
          <span class="settings-icon">⚙</span>
          <span>Settings</span>
        </button>
      </div>
    </aside>

    <main class="chat" id="section-chat">
      <div class="chat-header">
        <strong>AI Chat</strong>
        <span>Thinking-first AI</span>
      </div>

      <div class="chat-body" id="chat">
        <div class="message ai">
          Welcome to <strong>Hike.ai</strong><br><br>
          I analyze decisions using debate, regret prediction, emotional alignment,
          and real-world context.<br><br>
          Ask me anything or describe a decision you're facing.
        </div>
      </div>

      <div class="agent-panel">
        <div class="agent-section">
          <div class="agent">
            <span>Debate AI</span>
            <span id="debateStatus">Ready</span>
          </div>
          <div class="agent">
            <span>Regret AI</span>
            <span id="regretStatus">Low Risk</span>
          </div>
          <div class="agent">
            <span>Empathy AI</span>
            <span id="empathyStatus">Normal</span>
          </div>
          <div class="agent">
            <span>News Flow AI</span>
            <span id="newsStatus">Monitoring</span>
          </div>
        </div>

        <div class="agent-section">
          <div class="metric">
            <div class="metric-label">Confidence</div>
            <div class="bar">
              <div class="fill" id="confidenceFill" style="width:0%"></div>
            </div>
          </div>
          <div class="metric">
            <div class="metric-label">Regret Risk</div>
            <div class="bar">
              <div class="fill" id="regretFill" style="width:0%"></div>
            </div>
          </div>
        </div>
      </div>

      <div class="chat-input">
        <textarea id="input" placeholder="Describe your situation or decision…" rows="1"></textarea>
        <button id="sendBtn" onclick="send()">➤</button>
      </div>
    </main>

    <section class="content-section" id="section-projects">
      <div class="section-header">
        <h2 class="section-title">Projects</h2>
        <button class="btn-primary" onclick="openNewProjectModal()">+ New Project</button>
      </div>

      <div id="userProjectsContainer"></div>

      <div class="card">
        <div class="project-header">
          <span class="project-title">Career Transition 2026</span>
          <span class="project-status status-active">Active</span>
        </div>
        <div class="project-timeline">
          <div class="timeline-item high-risk">
            Jan 10 → Switch immediately ⚠️ High regret risk
          </div>
          <div class="timeline-item low-risk">
            Jan 18 → Delay & upskill ✓ Low regret
          </div>
          <div class="timeline-item positive">
            Feb 02 → Portfolio building 👍 Positive outcome
          </div>
        </div>
      </div>

      <div class="card">
        <div class="project-header">
          <span class="project-title">Startup Idea Validation</span>
          <span class="project-status status-paused">Paused</span>
        </div>
        <div class="project-timeline">
          <div class="timeline-item positive">
            Dec 28 → Market research completed
          </div>
          <div class="timeline-item">
            Jan 05 → Waiting for funding decision
          </div>
        </div>
      </div>

      <div class="card">
        <div class="project-header">
          <span class="project-title">Investment Portfolio Review</span>
          <span class="project-status status-completed">Completed</span>
        </div>
        <div class="project-timeline">
          <div class="timeline-item low-risk">
            Dec 15 → Diversification strategy approved ✓
          </div>
        </div>
      </div>
    </section>

    <!-- News Section -->
    <section class="content-section" id="section-news">
      <div class="section-header">
        <h2 class="section-title">Latest Global News</h2>
        <button class="refresh-news-btn" onclick="loadNews(true)">Refresh News</button>
      </div>
      <div id="news-loading" style="text-align: center; padding: 40px; color: #666; display: none;">
        Updating news feed...
      </div>
      <div id="news-container" class="news-grid">
        <!-- News items will be inserted here -->
      </div>
    </section>

    <section class="content-section" id="section-history">
      <div class="section-header">
        <h2 class="section-title">History</h2>
      </div>

      <div class="session-group">
        <div class="session-date">Today</div>
        <div class="history-item" onclick="showSection('chat')">
          <div>
            <div class="history-title">Career advice discussion</div>
            <span class="history-meta">Confidence: <span class="confidence-high">0.81</span> · Regret: Low</span>
          </div>
          <button class="btn-secondary">Continue as Project</button>
        </div>
        <div class="history-item" onclick="showSection('chat')">
          <div>
            <div class="history-title">Investment decision analysis</div>
            <span class="history-meta">Confidence: <span class="confidence-high">0.73</span> · Regret: Low</span>
          </div>
          <button class="btn-secondary">Continue as Project</button>
        </div>
      </div>

      <div class="session-group">
        <div class="session-date">Yesterday</div>
        <div class="history-item" onclick="showSection('chat')">
          <div>
            <div class="history-title">Market analysis for startup</div>
            <span class="history-meta">Confidence: <span class="confidence-low">0.47</span> · Regret: High</span>
          </div>
          <button class="btn-secondary">Continue as Project</button>
        </div>
      </div>

      <div class="session-group">
        <div class="session-date">Last Week</div>
        <div class="history-item" onclick="showSection('chat')">
          <div>
            <div class="history-title">Relationship advice</div>
            <span class="history-meta">Confidence: <span class="confidence-high">0.89</span> · Regret: Low</span>
          </div>
          <button class="btn-secondary">Continue as Project</button>
        </div>
        <div class="history-item" onclick="showSection('chat')">
          <div>
            <div class="history-title">Health decision support</div>
            <span class="history-meta">Confidence: <span class="confidence-high">0.76</span> · Regret: Low</span>
          </div>
          <button class="btn-secondary">Continue as Project</button>
        </div>
      </div>
    </section>
  </div>

  <div class="project-modal-overlay" id="projectModalOverlay" onclick="closeProjectModalOnOverlay(event)">
    <div class="project-modal" onclick="event.stopPropagation()">
      <div class="project-modal-header">
        <span class="project-modal-title">Create New Project</span>
        <button class="project-modal-close" onclick="closeProjectModal()">✕</button>
      </div>

      <div class="project-form-group">
        <label class="project-form-label">Project Title</label>
        <input type="text" class="project-form-input" id="projectTitle" placeholder="e.g., Career Decision 2026">
      </div>

      <div class="project-form-group">
        <label class="project-form-label">Description</label>
        <textarea class="project-form-input project-form-textarea" id="projectDescription"
          placeholder="What decision or goal are you exploring?"></textarea>
      </div>

      <div class="project-form-group">
        <label class="project-form-label">Status</label>
        <div class="project-status-selector">
          <div class="status-option active-status" id="status-active" onclick="selectProjectStatus('active')">
            Active
          </div>
          <div class="status-option" id="status-paused" onclick="selectProjectStatus('paused')">
            Paused
          </div>
        </div>
      </div>

      <div class="project-modal-actions">
        <button class="btn-cancel" onclick="closeProjectModal()">Cancel</button>
        <button class="btn-create" onclick="createProject()">Create Project</button>
      </div>
    </div>
  </div>

  <div class="settings-overlay" id="settingsOverlay" onclick="closeSettingsOnOverlay(event)">
    <div class="settings-panel" onclick="event.stopPropagation()">
      <div class="settings-header">
        <span class="settings-title">Settings</span>
        <button class="settings-close" onclick="closeSettings()">✕</button>
      </div>

      <div class="settings-section">
        <div class="toggle-row">
          <div>
            <span class="settings-label">Dark Mode</span>
            <div class="settings-description">Switch between light and dark theme</div>
          </div>
          <label class="toggle-switch">
            <input type="checkbox" id="themeToggle" onchange="handleThemeToggle()">
            <span class="toggle-slider"></span>
          </label>
        </div>
      </div>

      <div class="settings-section">
        <span class="settings-label">AI Model</span>
        <div class="settings-description">Select the primary AI provider for responses</div>
        <select class="model-selector" id="modelSelector" onchange="handleModelChange()">
          <option value="auto">Auto (Best Available)</option>
          <option value="groq">Groq - Llama 3.3 70B</option>
          <option value="openrouter">OpenRouter - DeepSeek R1</option>
          <option value="bytez">Bytez - Llama 3.1 8B</option>
          <option value="chutes">Chutes - Mistral Small 3.1</option>
        </select>
      </div>

      <div class="settings-section">
        <div class="toggle-row">
          <div>
            <span class="settings-label">
              Web Research
              <span class="setting-status" id="researchStatus">Active</span>
            </span>
            <div class="settings-description">Enable real-time web search for context-aware responses</div>
          </div>
          <label class="toggle-switch">
            <input type="checkbox" id="researchToggle" checked onchange="handleResearchToggle()">
            <span class="toggle-slider"></span>
          </label>
        </div>
      </div>

      <div class="settings-section">
        <div class="toggle-row">
          <div>
            <span class="settings-label">
              Debate AI
              <span class="setting-status" id="debateSettingStatus">Active</span>
            </span>
            <div class="settings-description">Enable multi-perspective analysis with multiple AI models</div>
          </div>
          <label class="toggle-switch">
            <input type="checkbox" id="debateToggle" checked onchange="handleDebateToggle()">
            <span class="toggle-slider"></span>
          </label>
        </div>
        <div class="model-checkboxes" id="debateModels">
          <label class="model-checkbox selected" id="debate-groq">
            <input type="checkbox" checked onchange="handleDebateModelChange('groq')">
            <span class="model-checkbox-label">Groq<br><small>Llama 3.3 70B</small></span>
          </label>
          <label class="model-checkbox selected" id="debate-openrouter">
            <input type="checkbox" checked onchange="handleDebateModelChange('openrouter')">
            <span class="model-checkbox-label">OpenRouter<br><small>DeepSeek R1</small></span>
          </label>
          <label class="model-checkbox" id="debate-bytez">
            <input type="checkbox" onchange="handleDebateModelChange('bytez')">
            <span class="model-checkbox-label">Bytez<br><small>Llama 3.1 8B</small></span>
          </label>
          <label class="model-checkbox" id="debate-chutes">
            <input type="checkbox" onchange="handleDebateModelChange('chutes')">
            <span class="model-checkbox-label">Chutes<br><small>Mistral Small</small></span>
          </label>
          <div class="model-count-hint" style="grid-column: span 2;">Select 2-4 models for debate</div>
        </div>
      </div>

      <div class="settings-section">
        <div class="toggle-row">
          <div>
            <span class="settings-label">
              Regret AI
              <span class="setting-status" id="regretSettingStatus">Active</span>
            </span>
            <div class="settings-description">Enable decision analysis and regret prediction</div>
          </div>
          <label class="toggle-switch">
            <input type="checkbox" id="regretToggle" checked onchange="handleRegretToggle()">
            <span class="toggle-slider"></span>
          </label>
        </div>
        <div class="model-checkboxes" id="regretModels">
          <label class="model-checkbox selected" id="regret-groq">
            <input type="checkbox" checked onchange="handleRegretModelChange('groq')">
            <span class="model-checkbox-label">Groq<br><small>Llama 3.3 70B</small></span>
          </label>
          <label class="model-checkbox selected" id="regret-openrouter">
            <input type="checkbox" checked onchange="handleRegretModelChange('openrouter')">
            <span class="model-checkbox-label">OpenRouter<br><small>DeepSeek R1</small></span>
          </label>
          <label class="model-checkbox" id="regret-bytez">
            <input type="checkbox" onchange="handleRegretModelChange('bytez')">
            <span class="model-checkbox-label">Bytez<br><small>Llama 3.1 8B</small></span>
          </label>
          <label class="model-checkbox" id="regret-chutes">
            <input type="checkbox" onchange="handleRegretModelChange('chutes')">
            <span class="model-checkbox-label">Chutes<br><small>Mistral Small</small></span>
          </label>
          <div class="model-count-hint" style="grid-column: span 2;">Select 2-4 models for regret analysis</div>
        </div>
      </div>
    </div>
  </div>

  <script>
    window.addEventListener('DOMContentLoaded', () => {
        const loginBtn = document.getElementById('loginBtn');
        if (loginBtn) {
            loginBtn.addEventListener('click', (e) => {
                e.preventDefault();
                handleLogin();
            });
        }
        
        const signupBtn = document.getElementById('signupBtn');
        if (signupBtn) {
            signupBtn.addEventListener('click', (e) => {
                e.preventDefault();
                handleSignup();
            });
        }

        // Allow Enter key to submit
        const inputs = document.querySelectorAll('.login-container input');
        inputs.forEach(input => {
            input.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    if (document.getElementById('loginForm').classList.contains('hidden')) {
                        handleSignup();
                    } else {
                        handleLogin();
                    }
                }
            });
        });
    });

    function safeJsonParse(str, fallback) {
        try {
            return str ? JSON.parse(str) : fallback;
        } catch (e) {
            console.error("JSON Parse Error:", e);
            return fallback;
        }
    }

    const appSettings = {
        model: localStorage.getItem('selectedModel') || 'auto',
        useResearch: localStorage.getItem('useResearch') === 'true',
        useDebate: localStorage.getItem('useDebate') === 'true',
        debateModels: safeJsonParse(localStorage.getItem('debateModels'), ["groq", "openrouter"]),
        useRegret: localStorage.getItem('useRegret') === 'true',
        regretModels: safeJsonParse(localStorage.getItem('regretModels'), ["groq"])
    };

    function escapeHtml(text) {
        if (!text) return text;
        return text
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
    }

    window.addEventListener('DOMContentLoaded', checkSession);

    async function checkSession() {
      try {
        const res = await fetch('/api/profile');
        if (res.ok) {
          showApp();
        }
      } catch (e) {
        console.log('Not logged in');
      }
    }

    function showApp() {
      document.getElementById('loginPage').style.display = 'none';
      document.getElementById('mainApp').classList.add('active');
      loadUserProjects();
    }

    function showError(msg) {
      const errorDiv = document.getElementById('errorMessage');
      errorDiv.textContent = msg;
      errorDiv.classList.add('show');
      setTimeout(() => errorDiv.classList.remove('show'), 5000);
    }

    async function handleLogin() {
      const email = document.getElementById('loginEmail').value;
      const password = document.getElementById('loginPassword').value;
      const btn = document.getElementById('loginBtn');

      if (!email || !password) {
        showError('Please fill in all fields');
        return;
      }

      btn.disabled = true;
      btn.textContent = 'Signing in...';

      try {
        const res = await fetch('/api/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, password })
        });

        const data = await res.json();

        if (res.ok) {
          showApp();
        } else {
          showError(data.detail || 'Login failed');
        }
      } catch (e) {
        showError('Connection error. Please try again.');
      } finally {
        btn.disabled = false;
        btn.textContent = 'Sign In';
      }
    }

    async function handleSignup() {
      const name = document.getElementById('signupName').value;
      const email = document.getElementById('signupEmail').value;
      const password = document.getElementById('signupPassword').value;
      const btn = document.getElementById('signupBtn');

      if (!name || !email || !password) {
        showError('Please fill in all fields');
        return;
      }

      btn.disabled = true;
      btn.textContent = 'Creating account...';

      try {
        const res = await fetch('/api/signup', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name, email, password })
        });

        const data = await res.json();

        if (res.ok) {
          showApp();
        } else {
          showError(data.detail || 'Signup failed');
        }
      } catch (e) {
        showError('Connection error. Please try again.');
      } finally {
        btn.disabled = false;
        btn.textContent = 'Create Account';
      }
    }

    function toggleDark() {
      document.body.classList.toggle("dark");
      const isDark = document.body.classList.contains("dark");
      const lightIcon = document.querySelector('.theme-light');
      const darkIcon = document.querySelector('.theme-dark');
      if (lightIcon && darkIcon) {
        lightIcon.style.display = isDark ? 'none' : 'block';
        darkIcon.style.display = isDark ? 'block' : 'none';
      }
      localStorage.setItem('theme', isDark ? 'dark' : 'light');
    }

    if (localStorage.getItem('theme') === 'dark') {
      document.body.classList.add('dark');
      const lightIcon = document.querySelector('.theme-light');
      const darkIcon = document.querySelector('.theme-dark');
      if (lightIcon && darkIcon) {
        lightIcon.style.display = 'none';
        darkIcon.style.display = 'block';
      }
    }

    const appSettings = {
      model: localStorage.getItem('selectedModel') || 'auto',
      useResearch: localStorage.getItem('useResearch') !== 'false',
      useDebate: localStorage.getItem('useDebate') !== 'false',
      debateModels: JSON.parse(localStorage.getItem('debateModels') || '["groq", "openrouter"]'),
      useRegret: localStorage.getItem('useRegret') !== 'false',
      regretModels: JSON.parse(localStorage.getItem('regretModels') || '["groq", "openrouter"]')
    };

    function openSettings() {
      document.getElementById('settingsOverlay').classList.add('active');

      document.getElementById('themeToggle').checked = document.body.classList.contains('dark');

      document.getElementById('modelSelector').value = appSettings.model;

      document.getElementById('researchToggle').checked = appSettings.useResearch;
      updateResearchStatus();

      document.getElementById('debateToggle').checked = appSettings.useDebate;
      updateDebateStatus();
      syncDebateModels();

      document.getElementById('regretToggle').checked = appSettings.useRegret;
      updateRegretStatus();
      syncRegretModels();
    }

    function closeSettings() {
      document.getElementById('settingsOverlay').classList.remove('active');
    }

    function closeSettingsOnOverlay(event) {
      if (event.target === document.getElementById('settingsOverlay')) {
        closeSettings();
      }
    }

    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') {
        closeSettings();
      }
    });

    function handleThemeToggle() {
      toggleDark();
      document.getElementById('themeToggle').checked = document.body.classList.contains('dark');
    }

    function handleModelChange() {
      const model = document.getElementById('modelSelector').value;
      appSettings.model = model;
      localStorage.setItem('selectedModel', model);
      console.log('Model changed to:', model);
    }

    function handleResearchToggle() {
      const useResearch = document.getElementById('researchToggle').checked;
      appSettings.useResearch = useResearch;
      localStorage.setItem('useResearch', useResearch);
      updateResearchStatus();
    }

    function updateResearchStatus() {
      const statusEl = document.getElementById('researchStatus');
      if (appSettings.useResearch) {
        statusEl.textContent = 'Active';
        statusEl.classList.remove('inactive');
      } else {
        statusEl.textContent = 'Disabled';
        statusEl.classList.add('inactive');
      }
    }

    function handleDebateToggle() {
      const useDebate = document.getElementById('debateToggle').checked;
      appSettings.useDebate = useDebate;
      localStorage.setItem('useDebate', useDebate);
      updateDebateStatus();
    }

    function updateDebateStatus() {
      const statusEl = document.getElementById('debateSettingStatus');
      const modelsEl = document.getElementById('debateModels');
      if (appSettings.useDebate) {
        statusEl.textContent = `${appSettings.debateModels.length} models`;
        statusEl.classList.remove('inactive');
        modelsEl.classList.add('visible');
      } else {
        statusEl.textContent = 'Disabled';
        statusEl.classList.add('inactive');
        modelsEl.classList.remove('visible');
      }
    }

    function syncDebateModels() {
      ['groq', 'openrouter', 'bytez', 'chutes'].forEach(model => {
        const checkbox = document.querySelector(`input[name="debate-model"][value="${model}"]`);
        const label = document.getElementById(`debate-${model}`);
        const isSelected = appSettings.debateModels.includes(model);
        checkbox.checked = isSelected;
        label.classList.toggle('selected', isSelected);
      });
    }

    function handleDebateModelChange(model) {
      const checkbox = document.querySelector(`input[name="debate-model"][value="${model}"]`);
      const label = document.getElementById(`debate-${model}`);

      if (checkbox.checked) {
        if (appSettings.debateModels.length < 4) {
          appSettings.debateModels.push(model);
          label.classList.add('selected');
        } else {
          checkbox.checked = false;
          alert('Maximum 4 models allowed for debate');
        }
      } else {
        if (appSettings.debateModels.length > 2) {
          appSettings.debateModels = appSettings.debateModels.filter(m => m !== model);
          label.classList.remove('selected');
        } else {
          checkbox.checked = true;
          alert('Minimum 2 models required for debate');
        }
      }

      localStorage.setItem('debateModels', JSON.stringify(appSettings.debateModels));
      updateDebateStatus();
    }

    function handleRegretToggle() {
      const useRegret = document.getElementById('regretToggle').checked;
      appSettings.useRegret = useRegret;
      localStorage.setItem('useRegret', useRegret);
      updateRegretStatus();
    }

    function updateRegretStatus() {
      const statusEl = document.getElementById('regretSettingStatus');
      const modelsEl = document.getElementById('regretModels');
      if (appSettings.useRegret) {
        statusEl.textContent = `${appSettings.regretModels.length} models`;
        statusEl.classList.remove('inactive');
        modelsEl.classList.add('visible');
      } else {
        statusEl.textContent = 'Disabled';
        statusEl.classList.add('inactive');
        modelsEl.classList.remove('visible');
      }
    }

    function syncRegretModels() {
      ['groq', 'openrouter', 'bytez', 'chutes'].forEach(model => {
        const checkbox = document.querySelector(`input[name="regret-model"][value="${model}"]`);
        const label = document.getElementById(`regret-${model}`);
        const isSelected = appSettings.regretModels.includes(model);
        checkbox.checked = isSelected;
        label.classList.toggle('selected', isSelected);
      });
    }

    function handleRegretModelChange(model) {
      const checkbox = document.querySelector(`input[name="regret-model"][value="${model}"]`);
      const label = document.getElementById(`regret-${model}`);

      if (checkbox.checked) {
        if (appSettings.regretModels.length < 4) {
          appSettings.regretModels.push(model);
          label.classList.add('selected');
        } else {
          checkbox.checked = false;
          alert('Maximum 4 models allowed for regret analysis');
        }
      } else {
        if (appSettings.regretModels.length > 2) {
          appSettings.regretModels = appSettings.regretModels.filter(m => m !== model);
          label.classList.remove('selected');
        } else {
          checkbox.checked = true;
          alert('Minimum 2 models required for regret analysis');
        }
      }

      localStorage.setItem('regretModels', JSON.stringify(appSettings.regretModels));
      updateRegretStatus();
    }

    function getSelectedProviders() {
      const model = appSettings.model;
      if (model === 'auto') {
        return ['groq', 'openrouter'];
      }
      return [model];
    }

    let selectedProjectStatus = 'active';
    let userProjects = JSON.parse(localStorage.getItem('userProjects') || '[]');

    function openNewProjectModal() {
      document.getElementById('projectModalOverlay').classList.add('active');
      document.getElementById('projectTitle').value = '';
      document.getElementById('projectDescription').value = '';
      selectProjectStatus('active');
    }

    function closeProjectModal() {
      document.getElementById('projectModalOverlay').classList.remove('active');
    }

    function closeProjectModalOnOverlay(event) {
      if (event.target === document.getElementById('projectModalOverlay')) {
        closeProjectModal();
      }
    }

    function selectProjectStatus(status) {
      selectedProjectStatus = status;
      document.getElementById('status-active').classList.remove('active-status');
      document.getElementById('status-paused').classList.remove('paused-status');

      if (status === 'active') {
        document.getElementById('status-active').classList.add('active-status');
      } else {
        document.getElementById('status-paused').classList.add('paused-status');
      }
    }

    function createProject() {
      const title = document.getElementById('projectTitle').value.trim();
      const description = document.getElementById('projectDescription').value.trim();

      if (!title) {
        alert('Please enter a project title');
        return;
      }

      const project = {
        id: Date.now(),
        title: title,
        description: description,
        status: selectedProjectStatus,
        createdAt: new Date().toISOString(),
        timeline: [
          {
            date: new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
            text: 'Project created',
            type: 'positive'
          }
        ]
      };

      userProjects.unshift(project);
      localStorage.setItem('userProjects', JSON.stringify(userProjects));
      renderUserProjects();
      closeProjectModal();
    }

    function deleteProject(projectId) {
      if (confirm('Are you sure you want to delete this project?')) {
        userProjects = userProjects.filter(p => p.id !== projectId);
        localStorage.setItem('userProjects', JSON.stringify(userProjects));
        renderUserProjects();
      }
    }

    function renderUserProjects() {
      const container = document.getElementById('userProjectsContainer');
      if (userProjects.length === 0) {
        container.innerHTML = '';
        return;
      }

      container.innerHTML = userProjects.map(project => `
        <div class="card">
          <div class="project-header">
            <span class="project-title">${escapeHtml(project.title)}</span>
            <span class="project-status status-${project.status}">${project.status.charAt(0).toUpperCase() + project.status.slice(1)}</span>
            <button class="project-delete-btn" onclick="deleteProject(${project.id})">Delete</button>
          </div>
          ${project.description ? `<div style="color: #666; font-size: 13px; margin-bottom: 12px;">${escapeHtml(project.description)}</div>` : ''}
          <div class="project-timeline">
            ${project.timeline.map(item => `
              <div class="timeline-item ${item.type || ''}">
                ${item.date} → ${escapeHtml(item.text)}
              </div>
            `).join('')}
          </div>
        </div>
      `).join('');
    }

    function escapeHtml(text) {
      const div = document.createElement('div');
      div.textContent = text;
      return div.innerHTML;
    }

    function loadUserProjects() {
      renderUserProjects();
    }

    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') {
        closeProjectModal();
      }
    });

    function togglePassword(inputId, textElement) {
      const input = document.getElementById(inputId);
      if (input.type === 'password') {
        input.type = 'text';
        textElement.textContent = 'Hide';
      } else {
        input.type = 'password';
        textElement.textContent = 'Show';
      }
    }

    function switchToSignup() {
      document.getElementById('loginForm').classList.add('hidden');
      document.getElementById('signupForm').classList.remove('hidden');
      document.getElementById('errorMessage').classList.remove('show');
    }

    function switchToLogin() {
      document.getElementById('signupForm').classList.add('hidden');
      document.getElementById('loginForm').classList.remove('hidden');
      document.getElementById('errorMessage').classList.remove('show');
    }

    async function loadNews(forceRefresh = false) {
      const container = document.getElementById('news-container');
      const loading = document.getElementById('news-loading');

      // If we already have news and not forcing refresh, don't reload
      if (container.children.length > 0 && !forceRefresh) return;

      loading.style.display = 'block';
      if (forceRefresh) container.innerHTML = '';

      try {
        const response = await fetch('/api/news/latest');
        const articles = await response.json();

        loading.style.display = 'none';

        if (!articles || articles.length === 0) {
          container.innerHTML = '<div style="grid-column: 1/-1; text-align: center; color: #666;">No news available at the moment.</div>';
          return;
        }

        container.innerHTML = articles.map(article => {
          const rawImage = article.urlToImage || 'https://via.placeholder.com/300x160?text=News';
          const image = rawImage.replace(/'/g, "%27");
          const safeUrl = (article.url || '').replace(/'/g, "%27");
          const date = new Date(article.publishedAt).toLocaleDateString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });

          return `
            <div class="news-card" onclick="window.open('${safeUrl}', '_blank')">
              <div class="news-image" style="background-image: url('${image}')">
                <span class="news-badge">${article.source?.name || 'News'}</span>
              </div>
              <div class="news-content">
                <div class="news-title">${article.title}</div>
                <div class="news-summary">${article.description || 'No description available.'}</div>
                <div class="news-meta">
                  <span class="news-source">${article.source?.name || 'Unknown Source'}</span>
                  <span>${date}</span>
                </div>
              </div>
            </div>
          `;
        }).join('');

      } catch (error) {
        console.error('Error loading news:', error);
        loading.style.display = 'none';
        container.innerHTML = '<div style="grid-column: 1/-1; text-align: center; color: #ef4444;">Failed to load news. Please try again.</div>';
      }
    }

    function showSection(section) {
      // Hide all sections
      document.getElementById('section-chat').style.display = 'none';

      ['projects', 'news', 'history'].forEach(sec => {
        const el = document.getElementById('section-' + sec);
        if (el) {
          el.style.display = 'none';
          el.classList.remove('active');
        }
        const nav = document.getElementById('nav-' + sec);
        if (nav) nav.classList.remove('active');
      });
      document.getElementById('nav-chat').classList.remove('active');

      // Show selected section
      if (section === 'chat') {
        document.getElementById('section-chat').style.display = 'flex';
        document.getElementById('nav-chat').classList.add('active');
      } else {
        const el = document.getElementById('section-' + section);
        if (el) {
          el.style.display = 'block';
          el.classList.add('active');
          document.getElementById('nav-' + section).classList.add('active');
        }

        if (section === 'news') {
          loadNews();
        }
      }
    }

    function showChat() {
      showSection('chat');
    }

    async function send() {
      const input = document.getElementById("input");
      const message = input.value.trim();
      if (!message) return;

      const chat = document.getElementById("chat");
      const sendBtn = document.getElementById("sendBtn");

      const userMsg = document.createElement("div");
      userMsg.className = "message user";
      userMsg.textContent = message;
      chat.appendChild(userMsg);

      input.value = "";
      input.style.height = 'auto';
      sendBtn.disabled = true;
      chat.scrollTop = chat.scrollHeight;

      document.getElementById('debateStatus').textContent = 'Thinking...';
      document.getElementById('empathyStatus').textContent = 'Analyzing...';
      document.getElementById('regretStatus').textContent = 'Predicting...';
      document.getElementById('newsStatus').textContent = 'Searching...';

      const aiMsg = document.createElement("div");
      aiMsg.className = "message ai";
      aiMsg.textContent = "Analyzing your request across all systems...";
      chat.appendChild(aiMsg);
      chat.scrollTop = chat.scrollHeight;

      try {

        const response = await fetch('/api/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            message,
            selected_model: appSettings.model,
            use_research: appSettings.useResearch,
            use_debate: appSettings.useDebate,
            debate_models: appSettings.debateModels,
            use_regret: appSettings.useRegret,
            regret_models: appSettings.regretModels
          })
        });
        const data = await response.json();

        aiMsg.textContent = data.response;

        const orchestration = data.orchestration || {};
        const details = data.details || {};

        let confidenceScore = 25;
        if (orchestration.research?.available) confidenceScore += 25;
        if (orchestration.debate?.available) confidenceScore += 25;
        if (orchestration.regret) confidenceScore += 25;

        document.getElementById('confidenceFill').style.width = confidenceScore + '%';

        if (orchestration.regret?.regret) {
          document.getElementById('regretFill').style.width = (orchestration.regret.regret * 10) + '%';
        }

        document.getElementById('debateStatus').textContent =
          orchestration.debate?.available ? `${orchestration.debate.providers?.length || 0} models` : 'Ready';
        document.getElementById('empathyStatus').textContent = details.emotion || 'Normal';
        document.getElementById('regretStatus').textContent =
          orchestration.regret ? (orchestration.regret.regret > 5 ? 'High Risk' : 'Low Risk') : 'Ready';
        document.getElementById('newsStatus').textContent =
          details.used_research ? 'Research Active' : 'Monitoring';

      } catch (e) {
        aiMsg.textContent = "Sorry, I encountered an error processing your request. Please try again.";
        console.error(e);

        document.getElementById('debateStatus').textContent = 'Error';
        document.getElementById('empathyStatus').textContent = 'Error';
        document.getElementById('regretStatus').textContent = 'Error';
        document.getElementById('newsStatus').textContent = 'Error';
      } finally {
        sendBtn.disabled = false;
        chat.scrollTop = chat.scrollHeight;
      }
    }

    document.getElementById('input').addEventListener('input', function () {
      this.style.height = 'auto';
      this.style.height = Math.min(this.scrollHeight, 120) + 'px';
    });

    document.getElementById('input').addEventListener('keydown', function (e) {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        send();
      }
    });
  </script>
</body>

</html>
"""

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

redis_db = None
if REDIS_AVAILABLE:
    try:
        redis_db = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        redis_db.ping()
        logger.info("Redis connected")
    except Exception as e:
        logger.warning(f"Redis connection failed: {e}. Some features may be degraded.")
        redis_db = None

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, index=True)
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
    id = Column(String, primary_key=True)
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
        return len(text) // 4

    @staticmethod
    def optimize_context(context: str, max_tokens: int = 1500) -> str:
        """Truncates context to a safe token limit while preserving the end (most recent)."""
        if not context: return ""
        
        current_tokens = TokenOptimizer.count_tokens(context)
        if current_tokens <= max_tokens:
            return context
            
        chars_to_keep = max_tokens * 4
        truncated = context[-chars_to_keep:]
        
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
        self.configs = {
            "groq": (5, 0.5),
            "openrouter": (5, 0.5),
            "chutes": (5, 0.5),
            "bytez": (5, 0.5),
            "tavily": (3, 0.5),
            "newsapi": (3, 0.5),
            "default": (5, 0.5)
        }
        self.buckets = defaultdict(lambda: {"tokens": 10.0, "last_update": time.time()})
        self._lock = asyncio.Lock()
        
    async def wait_if_needed(self, provider: str):
        config = self.configs.get(provider, self.configs["default"])
        max_tokens, refill_rate = config
        
        async with self._lock:
            bucket = self.configs.get(provider)
            key = provider if provider in self.configs else "default"
            bucket = self.buckets[key]
            
            now = time.time()
            elapsed = now - bucket["last_update"]
            
            new_tokens = elapsed * refill_rate
            bucket["tokens"] = min(max_tokens, bucket["tokens"] + new_tokens)
            bucket["last_update"] = now
            
            if bucket["tokens"] >= 1:
                bucket["tokens"] -= 1
                return
            
            required = 1.0 - bucket["tokens"]
            wait_time = required / refill_rate
            
        if wait_time > 0:
            logger.warning(f"Rate limit hit for {provider}, waiting {wait_time:.2f}s")
            await asyncio.sleep(wait_time)
            
            await self.wait_if_needed(provider)

rate_limiter = ProviderRateLimiter()

class UserRateLimiter:
    def __init__(self, requests_per_minute: int = 60):
        self.rate = requests_per_minute
        self.history = defaultdict(list)
        
    def check_rate_limit(self, client_ip: str):
        now = time.time()
        self.history[client_ip] = [t for t in self.history[client_ip] if now - t < 60]
        
        if len(self.history[client_ip]) >= self.rate:
            raise HTTPException(status_code=429, detail="Too many requests. Please slow down.")
            
        self.history[client_ip].append(now)

user_limiter = UserRateLimiter(requests_per_minute=10)

def verify_rate_limit(request: Request):
    """Dependency for API routes"""
    client_ip = request.client.host
    user_limiter.check_rate_limit(client_ip)

class BudgetManager:
    """
    Manages budgets for API calls, tokens, and execution time.
    Provides safe limits to prevent runaway costs or timeouts.
    """
    
    def __init__(self):
        self.hourly_limits = {
            "research": int(os.getenv("BUDGET_RESEARCH_HOURLY", 10)),
            "debate": int(os.getenv("BUDGET_DEBATE_HOURLY", 8)),
            "regret": int(os.getenv("BUDGET_REGRET_HOURLY", 15)),
            "empathy": int(os.getenv("BUDGET_EMPATHY_HOURLY", 30)),
            "news": int(os.getenv("BUDGET_NEWS_HOURLY", 20)),
        }
        
        self.token_limits = {
            "input_max": int(os.getenv("BUDGET_INPUT_TOKENS", 20)),
            "output_max": int(os.getenv("BUDGET_OUTPUT_TOKENS", 10)),
            "context_max": int(os.getenv("BUDGET_CONTEXT_TOKENS", 40)), 
        }
        
        self.time_limits = {
            "single_agent": float(os.getenv("BUDGET_AGENT_TIMEOUT", 10.0)),
            "total_request": float(os.getenv("BUDGET_REQUEST_TIMEOUT", 30.0)),
            "debate_round": float(os.getenv("BUDGET_DEBATE_TIMEOUT", 15.0)),
        }
        
        self._usage: Dict[str, Dict[str, list]] = defaultdict(lambda: defaultdict(list))
        self._lock = threading.Lock()
    
    def check_budget(self, user_id: str, agent: str) -> tuple[bool, str]:
        """
        Check if user has budget remaining for an agent.
        Returns (allowed: bool, reason: str)
        """
        limit = self.hourly_limits.get(agent, 50)
        now = time.time()
        hour_ago = now - 3600
        
        with self._lock:
            self._usage[user_id][agent] = [
                (ts, count) for ts, count in self._usage[user_id][agent]
                if ts > hour_ago
            ]
            
            total_usage = sum(count for _, count in self._usage[user_id][agent])
            
            if total_usage >= limit:
                return False, f"Hourly limit ({limit}) reached for {agent}"
            
            return True, f"Budget OK: {total_usage}/{limit} used"
    
    def record_usage(self, user_id: str, agent: str, count: int = 1):
        """Record API usage for budget tracking."""
        with self._lock:
            self._usage[user_id][agent].append((time.time(), count))
    
    def get_remaining(self, user_id: str, agent: str) -> int:
        """Get remaining budget for an agent."""
        allowed, _ = self.check_budget(user_id, agent)
        if not allowed:
            return 0
        
        limit = self.hourly_limits.get(agent, 50)
        hour_ago = time.time() - 3600
        
        with self._lock:
            total_usage = sum(
                count for ts, count in self._usage[user_id][agent]
                if ts > hour_ago
            )
        
        return max(0, limit - total_usage)
    
    def get_all_budgets(self, user_id: str) -> Dict[str, Dict]:
        """Get budget status for all agents."""
        return {
            agent: {
                "limit": limit,
                "remaining": self.get_remaining(user_id, agent),
                "timeout": self.time_limits.get("single_agent", 10.0)
            }
            for agent, limit in self.hourly_limits.items()
        }

class IntentClassifier:
    """
    Rule-based intent classification using explicit Python patterns.
    No LLM calls - purely deterministic.
    """
    
    INTENT_PATTERNS = {
        "emergency": {
            "keywords": ["help", "urgent", "emergency", "asap", "immediately", "crisis"],
            "patterns": [r"\bhelp\s+me\b", r"\burgent\b", r"\bcall\s+911\b"],
            "priority": 100,
        },
        "decision": {
            "keywords": ["should i", "decide", "choice", "option", "better", "versus", "vs", "or should"],
            "patterns": [r"\bshould\s+i\b", r"\bwhich\s+(one|option)\b", r"\bbetter\s+to\b"],
            "priority": 80,
        },
        "emotional_support": {
            "keywords": ["feel", "sad", "depressed", "anxious", "worried", "scared", "lonely", "stressed", "overwhelmed"],
            "patterns": [r"\bi\s+feel\b", r"\bi'm\s+(so\s+)?(sad|depressed|anxious)", r"\bcan't\s+cope\b"],
            "priority": 75,
        },
        "advice": {
            "keywords": ["advice", "recommend", "suggest", "what would you", "how should"],
            "patterns": [r"\bwhat\s+(do\s+you|would\s+you)\s+recommend\b", r"\bany\s+advice\b"],
            "priority": 70,
        },
        "information_seeking": {
            "keywords": ["what is", "how does", "explain", "tell me about", "define", "meaning of"],
            "patterns": [r"\bwhat\s+is\b", r"\bhow\s+does\b", r"\bexplain\b", r"\btell\s+me\s+about\b"],
            "priority": 60,
        },
        "debate": {
            "keywords": ["argue", "debate", "pros and cons", "both sides", "controversial", "opinion on"],
            "patterns": [r"\bpros\s+and\s+cons\b", r"\bboth\s+sides\b", r"\bdebate\b"],
            "priority": 65,
        },
        "news": {
            "keywords": ["news", "latest", "today", "current events", "happening", "recent"],
            "patterns": [r"\b(latest|recent|current)\s+news\b", r"\bwhat's\s+happening\b"],
            "priority": 55,
        },
        "question": {
            "keywords": ["what", "how", "why", "when", "where", "who", "which", "can you"],
            "patterns": [r"\?$", r"^(what|how|why|when|where|who|which)\b"],
            "priority": 40,
        },
        "casual": {
            "keywords": ["hello", "hi", "hey", "thanks", "bye", "good morning", "how are you"],
            "patterns": [r"^(hi|hello|hey)\b", r"\bthanks?\b", r"\bgoodbye\b"],
            "priority": 20,
        },
    }
    
    EMOTION_PATTERNS = {
        "sadness": ["sad", "empty", "lost", "depressed", "lonely", "crying", "tears", "hopeless", "hurt", "broken", "grief"],
        "anger": ["angry", "mad", "furious", "annoyed", "frustrated", "hate", "pissed", "rage", "irritated"],
        "fear": ["fear", "anxious", "scared", "worried", "nervous", "panic", "stress", "overwhelmed", "terrified"],
        "joy": ["happy", "good", "great", "excited", "wonderful", "amazing", "love", "joy", "blessed", "thrilled"],
        "seeking_advice": ["what should", "how do", "how can", "what can", "advice", "help me", "recommend"],
    }
    
    @classmethod
    def classify_intent(cls, message: str) -> Dict[str, any]:
        """
        Classify user intent using deterministic rules.
        Returns intent with confidence score and matched patterns.
        """
        msg_lower = message.lower().strip()
        
        matches = []
        for intent, config in cls.INTENT_PATTERNS.items():
            score = 0
            matched_keywords = []
            matched_patterns = []
            
            for keyword in config["keywords"]:
                if keyword in msg_lower:
                    score += 10
                    matched_keywords.append(keyword)
            
            for pattern in config["patterns"]:
                if re.search(pattern, msg_lower, re.IGNORECASE):
                    score += 20
                    matched_patterns.append(pattern)
            
            if score > 0:
                weighted_score = score * (config["priority"] / 100)
                matches.append({
                    "intent": intent,
                    "score": weighted_score,
                    "raw_score": score,
                    "keywords": matched_keywords,
                    "patterns": matched_patterns,
                    "priority": config["priority"],
                })
        
        matches.sort(key=lambda x: (x["score"], x["priority"]), reverse=True)
        
        if matches:
            best = matches[0]
            return {
                "intent": best["intent"],
                "confidence": min(1.0, best["score"] / 50),
                "matched_keywords": best["keywords"],
                "matched_patterns": best["patterns"],
                "all_matches": matches[:3],
            }
        
        return {
            "intent": "casual",
            "confidence": 0.3,
            "matched_keywords": [],
            "matched_patterns": [],
            "all_matches": [],
        }
    
    @classmethod
    def detect_emotion(cls, message: str) -> Dict[str, any]:
        """Detect emotion using keyword matching."""
        msg_lower = message.lower()
        
        emotion_scores = {}
        for emotion, keywords in cls.EMOTION_PATTERNS.items():
            score = sum(1 for kw in keywords if kw in msg_lower)
            if score > 0:
                emotion_scores[emotion] = score
        
        if emotion_scores:
            best_emotion = max(emotion_scores, key=emotion_scores.get)
            return {
                "primary": best_emotion,
                "confidence": min(1.0, emotion_scores[best_emotion] / 3),
                "all_emotions": emotion_scores,
            }
        
        return {
            "primary": "neutral",
            "confidence": 0.5,
            "all_emotions": {},
        }

class AgentPriority:
    """Defines execution priority for agents."""
    EMERGENCY = 100
    RESEARCH = 80
    EMPATHY = 70
    DEBATE = 60
    REGRET = 50
    NEWS = 40

class DeterministicOrchestrator:
    """
    Central orchestrator using deterministic Python rules to:
    1. Understand user context and intent (rule-based, no LLM)
    2. Control agent execution with budgets and priorities
    3. Coordinate responses from all modules safely
    4. Provide full transparency into decision-making
    
    Architecture:
    - Research/Real-time Data: Tavily API
    - News: NewsAPI
    - Debate: All models (Groq, OpenRouter, Bytez, Chutes)
    - Empathy: User-selected model
    - Regret AI: Decision analysis models
    - Orchestration: THIS CLASS (deterministic Python rules)
    
    Key Principles:
    - No LLM calls for orchestration decisions
    - Explicit, auditable rules
    - Budget enforcement
    - Safe fallbacks and circuit breakers
    """
    
    def __init__(self):
        self.budget_manager = BudgetManager()
        self.intent_classifier = IntentClassifier()
        
        self._circuit_breakers: Dict[str, Dict] = defaultdict(
            lambda: {"failures": 0, "last_failure": 0, "open": False}
        )
        self._circuit_threshold = 3
        self._circuit_reset_time = 60
        
        logger.info("Deterministic Orchestrator initialized (no LLM dependencies)")
    
    def _check_circuit(self, agent: str) -> bool:
        """Check if circuit breaker allows agent execution."""
        cb = self._circuit_breakers[agent]
        
        if cb["open"]:
            if time.time() - cb["last_failure"] > self._circuit_reset_time:
                cb["open"] = False
                cb["failures"] = 0
                logger.info(f"Circuit breaker reset for {agent}")
                return True
            return False
        
        return True
    
    def _record_failure(self, agent: str):
        """Record agent failure for circuit breaker."""
        cb = self._circuit_breakers[agent]
        cb["failures"] += 1
        cb["last_failure"] = time.time()
        
        if cb["failures"] >= self._circuit_threshold:
            cb["open"] = True
            logger.warning(f"Circuit breaker OPEN for {agent} after {cb['failures']} failures")
    
    def _record_success(self, agent: str):
        """Record agent success, reset failure count."""
        cb = self._circuit_breakers[agent]
        cb["failures"] = 0
    
    def analyze_context(self, user_message: str, conversation_history: List[Dict] = None, user_id: str = "anonymous") -> Dict:
        """
        Analyze user message using deterministic rules.
        Returns structured instructions for all systems.
        
        This method uses NO LLM calls - purely Python logic.
        """
        intent_result = self.intent_classifier.classify_intent(user_message)
        intent = intent_result["intent"]
        intent_confidence = intent_result["confidence"]
        
        emotion_result = self.intent_classifier.detect_emotion(user_message)
        emotion = emotion_result["primary"]
        
        msg_lower = user_message.lower()
        msg_length = len(user_message)
        has_question_mark = "?" in user_message
        
        needs_research = self._should_research(intent, msg_lower, msg_length, user_id)
        needs_news = self._should_fetch_news(intent, msg_lower, user_id)
        needs_debate = self._should_debate(intent, msg_lower, intent_confidence, user_id)
        needs_regret = self._should_analyze_regret(intent, msg_lower, user_id)
        
        research_query = self._extract_research_query(user_message, intent) if needs_research else ""
        
        debate_providers = self._select_debate_providers(user_id) if needs_debate else []
        
        news_keywords = self._extract_news_keywords(user_message) if needs_news else ""
        
        empathy_instruction = self._get_empathy_instruction(emotion, intent)
        
        synthesis_instruction = self._get_synthesis_instruction(intent, needs_research, needs_debate, needs_regret)
        
        return {
            "intent": intent,
            "intent_confidence": intent_confidence,
            "intent_details": intent_result,
            
            "needs_research": needs_research,
            "research_query": research_query,
            "research_priority": AgentPriority.RESEARCH if needs_research else 0,
            
            "needs_news": needs_news,
            "news_keywords": news_keywords,
            "news_priority": AgentPriority.NEWS if needs_news else 0,
            
            "debate_question": user_message if needs_debate else "",
            "debate_providers": debate_providers,
            "debate_priority": AgentPriority.DEBATE if needs_debate else 0,
            
            "emotion_detected": emotion,
            "emotion_confidence": emotion_result["confidence"],
            "emotion_details": emotion_result,
            "empathy_instruction": empathy_instruction,
            "empathy_priority": AgentPriority.EMPATHY,
            
            "needs_regret_analysis": needs_regret,
            "regret_context": user_message if needs_regret else "",
            "regret_priority": AgentPriority.REGRET if needs_regret else 0,
            
            "final_response_instruction": synthesis_instruction,
            
            "budget_status": self.budget_manager.get_all_budgets(user_id),
            
            "orchestration_method": "deterministic_rules",
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    def _should_research(self, intent: str, msg_lower: str, msg_length: int, user_id: str) -> bool:
        """Determine if research is needed using explicit rules."""
        allowed, _ = self.budget_manager.check_budget(user_id, "research")
        if not allowed:
            return False
        
        if not self._check_circuit("research"):
            return False
        
        research_intents = {"information_seeking", "question", "advice", "debate"}
        research_keywords = ["research", "find out", "look up", "search for", "what is the latest"]
        
        if intent in research_intents and msg_length > 15:
            return True
        
        if any(kw in msg_lower for kw in research_keywords):
            return True
        
        return False
    
    def _should_fetch_news(self, intent: str, msg_lower: str, user_id: str) -> bool:
        """Determine if news fetch is needed."""
        allowed, _ = self.budget_manager.check_budget(user_id, "news")
        if not allowed:
            return False
        
        if not self._check_circuit("news"):
            return False
        
        news_keywords = ["news", "latest", "today", "current events", "happening now", "recent", "headlines"]
        return intent == "news" or any(kw in msg_lower for kw in news_keywords)
    
    def _should_debate(self, intent: str, msg_lower: str, confidence: float, user_id: str) -> bool:
        """Determine if multi-model debate is needed."""
        allowed, _ = self.budget_manager.check_budget(user_id, "debate")
        if not allowed:
            return False
        
        if not self._check_circuit("debate"):
            return False
        
        debate_intents = {"debate", "decision", "advice"}
        debate_keywords = ["pros and cons", "both sides", "different perspective", "compare", "versus", " vs "]
        
        if intent in debate_intents and confidence > 0.5:
            return True
        
        if any(kw in msg_lower for kw in debate_keywords):
            return True
        
        return False
    
    def _should_analyze_regret(self, intent: str, msg_lower: str, user_id: str) -> bool:
        """Determine if regret analysis is needed."""
        allowed, _ = self.budget_manager.check_budget(user_id, "regret")
        if not allowed:
            return False
        
        if not self._check_circuit("regret"):
            return False
        
        decision_keywords = [
            "should i", "decision", "choice", "option", "considering", 
            "thinking about", "planning to", "going to", "want to"
        ]
        
        return intent == "decision" or any(kw in msg_lower for kw in decision_keywords)
    
    def _extract_research_query(self, message: str, intent: str) -> str:
        """Extract an optimized research query from the message."""
        fillers = ["please", "can you", "could you", "i want to know", "tell me", "help me"]
        query = message.lower()
        for filler in fillers:
            query = query.replace(filler, "")
        
        query = " ".join(query.split())
        query = query.strip("?.,! ")
        
        return query[:200] if query else message[:200]
    
    def _extract_news_keywords(self, message: str) -> str:
        """Extract news-relevant keywords from message."""
        stopwords = {"the", "a", "an", "is", "are", "was", "were", "what", "about", "news", "latest", "tell", "me"}
        words = message.lower().split()
        keywords = [w.strip("?.,!") for w in words if w.lower() not in stopwords and len(w) > 2]
        
        return " ".join(keywords[:5])
    
    def _select_debate_providers(self, user_id: str) -> List[str]:
        """Select available debate providers based on configuration and budget."""
        available_providers = []
        
        provider_configs = [
            ("groq", GROQ_API_KEY),
            ("openrouter", OPENROUTER_API_KEY),
            ("chutes", CHUTES_API_KEY),
            ("bytez", BYTEZ_API_KEY),
        ]
        
        for provider, key in provider_configs:
            if key and self._check_circuit(provider):
                available_providers.append(provider)
        
        if len(available_providers) >= 2:
            return available_providers[:4]
        elif available_providers:
            return available_providers
        else:
            return ["groq", "openrouter"]
    
    def _get_empathy_instruction(self, emotion: str, intent: str) -> str:
        """Get empathy instruction based on emotion and intent."""
        instructions = {
            "sadness": "Respond with warmth and validation. Acknowledge their pain without minimizing it.",
            "anger": "Validate their frustration. Help them feel heard before offering any perspective.",
            "fear": "Provide reassurance and calm presence. Focus on what they can control.",
            "joy": "Celebrate with them! Match their positive energy.",
            "seeking_advice": "Listen carefully, then offer thoughtful guidance. Ask clarifying questions if needed.",
            "neutral": "Be warm and attentive. Follow their lead on the conversation.",
        }
        
        base = instructions.get(emotion, instructions["neutral"])
        
        if intent == "decision":
            base += " Help them think through the decision without pushing an agenda."
        elif intent == "emotional_support":
            base += " Prioritize emotional connection over problem-solving."
        
        return base
    
    def _get_synthesis_instruction(self, intent: str, has_research: bool, has_debate: bool, has_regret: bool) -> str:
        """Generate instruction for synthesizing the final response."""
        parts = ["Create a helpful, conversational response that:"]
        
        parts.append("1. Directly addresses the user's message")
        
        if has_research:
            parts.append("2. Incorporates relevant research findings naturally")
        
        if has_debate:
            parts.append("3. Presents multiple perspectives fairly")
        
        if has_regret:
            parts.append("4. Includes decision analysis insights")
        
        parts.append(f"Focus on {intent.replace('_', ' ')} as the primary intent.")
        
        return " ".join(parts)
    
    def synthesize_response(self,
                           user_message: str,
                           orchestration: Dict,
                           research_data: Dict = None,
                           news_data: List = None,
                           debate_data: Dict = None,
                           empathy_response: str = None,
                           regret_data: Dict = None,
                           user_id: str = "anonymous") -> str:
        """
        Synthesize all AI responses into a coherent final response.
        Uses deterministic rules to combine responses - NO LLM for synthesis.
        """
        intent = orchestration.get("intent", "casual")
        parts = []
        
        if empathy_response:
            parts.append(empathy_response)
        
        if research_data and research_data.get("answer"):
            answer = research_data["answer"]
            if len(answer) > 50 and (not empathy_response or answer[:100] not in empathy_response):
                parts.append(f"\n\n**Research Insights:**\n{answer[:500]}")
                
                sources = research_data.get("sources", [])
                if sources:
                    source_links = [f"- [{s.get('title', 'Source')[:50]}]({s.get('url', '')})" 
                                   for s in sources[:3] if s.get('url')]
                    if source_links:
                        parts.append("\n**Sources:**\n" + "\n".join(source_links))
        
        if news_data and orchestration.get("needs_news"):
            news_items = news_data[:3] if isinstance(news_data, list) else []
            if news_items:
                news_text = "\n\n**Latest News:**\n"
                for item in news_items:
                    title = item.get("title", "")[:80]
                    if title:
                        news_text += f"• {title}\n"
                parts.append(news_text)
        
        if debate_data:
            final_answer = debate_data.get("final_answer", "")
            if final_answer:
                parts.append(f"\n\n**Multiple Perspectives:**\n{final_answer[:600]}")
            else:
                responses = debate_data.get("responses", {})
                if responses:
                    perspectives = []
                    for provider, resp in list(responses.items())[:3]:
                        if resp.get("success") and resp.get("response"):
                            perspectives.append(f"**{provider.title()}:** {resp['response'][:200]}...")
                    if perspectives:
                        parts.append("\n\n**Different Viewpoints:**\n" + "\n\n".join(perspectives))
        
        if regret_data and orchestration.get("needs_regret_analysis"):
            action = regret_data.get("action", "")
            regret_score = regret_data.get("regret", 0)
            domain = regret_data.get("domain", "")
            
            if action:
                risk_level = "Low" if regret_score < 3 else ("Medium" if regret_score < 6 else "High")
                parts.append(f"\n\n**Decision Analysis:**\n"
                           f"• Suggested action: {action}\n"
                           f"• Domain: {domain}\n"
                           f"• Risk level: {risk_level} (score: {regret_score:.1f}/10)")
        
        if parts:
            result = "\n".join(parts)
        else:
            result = "I'm here to help. Could you tell me more about what you're looking for?"
        
        self.budget_manager.record_usage(user_id, "empathy")
        
        return result
    
    def get_orchestration_summary(self, orchestration: Dict) -> Dict:
        """Get a human-readable summary of orchestration decisions."""
        return {
            "intent": orchestration.get("intent"),
            "confidence": orchestration.get("intent_confidence"),
            "agents_activated": {
                "research": orchestration.get("needs_research", False),
                "news": orchestration.get("needs_news", False),
                "debate": bool(orchestration.get("debate_providers")),
                "regret": orchestration.get("needs_regret_analysis", False),
                "empathy": True,
            },
            "emotion": orchestration.get("emotion_detected"),
            "method": "deterministic_rules",
        }

orchestrator = DeterministicOrchestrator()

class NewsSystem:
    def __init__(self):
        self.news_index = []
        self._model = None
        self.seen_urls = set()

    @property
    def model(self):
        if self._model is None and SENTENCE_TRANSFORMERS_AVAILABLE:
            self._model = SentenceTransformer(EMBEDDING_MODEL)
        return self._model

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
            model = genai.GenerativeModel("gemini-pro")
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
                embedding = []
                if self.model is not None:
                    embedding = self.model.encode([summary], normalize_embeddings=True)[0].tolist()
                
                self.news_index.append({
                    "text": summary,
                    "url": url,
                    "title": title,
                    "description": desc,
                    "urlToImage": article.get("urlToImage"),
                    "publishedAt": article.get("publishedAt"),
                    "source": article.get("source"),
                    "embedding": embedding,
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
        opt_msg = TokenOptimizer.optimize_context(message, max_tokens=250)
        
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
        self._encoder = None

    @property
    def encoder(self):
        if self._encoder is None and SENTENCE_TRANSFORMERS_AVAILABLE:
            self._encoder = SentenceTransformer("all-MiniLM-L6-v2")
        return self._encoder

    def predict_outcome(self, context: str, action: str) -> float:
        context_lower = context.lower()
        positive_words = ["good", "great", "happy", "success", "improve", "better", "love", "excited"]
        negative_words = ["bad", "sad", "fail", "worse", "hate", "worried", "stressed", "anxious"]
        
        score = 0
        for word in positive_words:
            if word in context_lower:
                score += 2
        for word in negative_words:
            if word in context_lower:
                score -= 2
        
        score += random.uniform(-3, 3)
        return max(-10, min(10, score))

    def make_decision(self, user_id: str, context: str, emotion: str) -> Dict:
        possible_actions = list(self.actions.keys())
        chosen_action = random.choice(possible_actions)
        
        score = self.predict_outcome(context, chosen_action)
        regret = max(0, 10 - score)

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
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["*"])
app.add_middleware(SecurityMiddleware)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)

LANDING_HTML = """
<!DOCTYPE html>
<html>
<head><title>Unified AI</title><style>body{font-family:sans-serif;background:
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
    return HTMLResponse(content=HTML_CONTENT)

@app.get("/login", response_class=HTMLResponse)
def login_redirect(request: Request):
    return HTMLResponse(content=HTML_CONTENT)

@app.get("/signup", response_class=HTMLResponse)
def signup_redirect(request: Request):
    return HTMLResponse(content=HTML_CONTENT)

@app.get("/news", response_class=HTMLResponse)
def news_ui(request: Request):
    return HTMLResponse(content=HTML_CONTENT)

@app.get("/debate", response_class=HTMLResponse)
def debate_ui(request: Request):
    return HTMLResponse(content=HTML_CONTENT)

@app.get("/regret", response_class=HTMLResponse)
def regret_ui(request: Request):
    return HTMLResponse(content=HTML_CONTENT)

@app.get("/chat", response_class=HTMLResponse)
def chat_ui(request: Request):
    return HTMLResponse(content=HTML_CONTENT)
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
    selected_model: str = "auto"
    use_research: bool = True
    use_debate: bool = True
    debate_models: List[str] = ["groq", "openrouter"]
    use_regret: bool = True
    regret_models: List[str] = ["groq", "openrouter"]

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
                    use_research=False
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
    providers: List[str] = ["groq", "openrouter", "bytez", "chutes"]
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