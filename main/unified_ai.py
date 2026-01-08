import multiprocessing
# Fix for semaphore leak warning on macOS
if __name__ != "__main__":
    try:
        multiprocessing.set_start_method('spawn', force=True)
    except RuntimeError:
        pass

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

HTML_CONTENT = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Hike.ai</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    :root { --bg: #fff; --bg-secondary: #f3f4f6; --text: #000; --text-muted: #6b7280; --border: #e5e7eb; --blue: #2563eb; }
    .dark { --bg: #000; --bg-secondary: #111; --text: #fff; --text-muted: #9ca3af; --border: #1f2937; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--bg); color: var(--text); height: 100vh; overflow: hidden; }
    .hidden { display: none !important; }
    
    /* Login */
    .login-page { min-height: 100vh; background: #000; display: flex; align-items: center; justify-content: center; padding: 1rem; }
    .login-wrapper { width: 100%; max-width: 28rem; }
    .login-header { text-align: center; margin-bottom: 2rem; }
    .login-logo { display: flex; align-items: center; justify-content: center; gap: 0.75rem; margin-bottom: 1rem; }
    .login-logo-icon { width: 3rem; height: 3rem; background: #fff; border-radius: 0.75rem; display: flex; align-items: center; justify-content: center; }
    .login-logo h1 { font-size: 3rem; font-weight: 700; color: #fff; }
    .login-subtitle { color: #9ca3af; }
    .login-card { background: #fff; border-radius: 1.5rem; padding: 2rem; box-shadow: 0 25px 50px -12px rgba(0,0,0,0.25); }
    .form-group { margin-bottom: 1.5rem; }
    .form-label { display: block; font-size: 0.875rem; font-weight: 500; color: #374151; margin-bottom: 0.5rem; }
    .form-input { width: 100%; padding: 0.75rem 1rem; border-radius: 9999px; border: 2px solid #d1d5db; font-size: 1rem; background: #fff; color: #000; }
    .form-input:focus { outline: none; border-color: #000; }
    .password-wrapper { position: relative; }
    .password-toggle { position: absolute; right: 1rem; top: 50%; transform: translateY(-50%); background: none; border: none; color: #6b7280; cursor: pointer; }
    .btn { width: 100%; padding: 0.75rem; border-radius: 9999px; font-weight: 600; cursor: pointer; border: none; }
    .btn-primary { background: #000; color: #fff; }
    .btn-google { background: #fff; color: #374151; border: 2px solid #d1d5db; display: flex; align-items: center; justify-content: center; gap: 0.75rem; margin-top: 1rem; }
    .divider { position: relative; margin: 1.5rem 0; }
    .divider-line { border-top: 1px solid #d1d5db; }
    .divider-text { position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); background: #fff; padding: 0 0.5rem; color: #6b7280; font-size: 0.875rem; }
    .login-footer { text-align: center; margin-top: 1.5rem; color: #4b5563; }
    .login-footer a { color: #000; font-weight: 600; cursor: pointer; }
    .error-message { background: #fef2f2; color: #b91c1c; padding: 0.75rem; border-radius: 0.5rem; margin-bottom: 1rem; display: none; }
    .error-message.show { display: block; }
    
    /* App */
    .app { display: none; height: 100vh; width: 100%; }
    .app.active { display: flex; }
    .sidebar { width: 16rem; background: var(--bg); border-right: 1px solid var(--border); display: flex; flex-direction: column; }
    .sidebar-header { padding: 1rem; border-bottom: 1px solid var(--border); }
    .sidebar-logo { display: flex; align-items: center; gap: 0.5rem; }
    .sidebar-logo-icon { padding: 0.5rem; border-radius: 0.5rem; }
    .dark .sidebar-logo-icon { background: #fff; }
    .sidebar-logo h1 { font-size: 1.25rem; font-weight: 700; }
    .sidebar-nav { flex: 1; overflow-y: auto; padding: 0.75rem; }
    .nav-item { width: 100%; display: flex; align-items: center; gap: 0.75rem; padding: 0.5rem 0.75rem; border-radius: 9999px; background: none; border: none; color: var(--text); font-size: 0.875rem; cursor: pointer; text-align: left; }
    .nav-item:hover, .nav-item.active { background: var(--bg-secondary); }
    .nav-item svg { width: 1.25rem; height: 1.25rem; flex-shrink: 0; }
    .sidebar-footer { padding: 0.75rem; border-top: 1px solid var(--border); }
    
    /* Projects List */
    .projects-list { margin-left: 2rem; margin-top: 0.25rem; }
    .project-item { display: flex; align-items: center; padding: 0.4rem 0.75rem; border-radius: 0.5rem; font-size: 0.8rem; color: var(--text-muted); cursor: pointer; }
    .project-item:hover { background: var(--bg-secondary); }
    .project-name { flex: 1; }
    .project-delete { opacity: 0; background: none; border: none; color: var(--text-muted); cursor: pointer; padding: 0.25rem; }
    .project-item:hover .project-delete { opacity: 1; }
    .project-delete:hover { color: #ef4444; }
    .create-project { color: var(--blue); }
    
    /* Settings */
    .settings-panel { margin-top: 0.5rem; padding: 0.75rem; background: var(--bg-secondary); border-radius: 0.5rem; max-height: 400px; overflow-y: auto; }
    .settings-section { margin-bottom: 1rem; }
    .settings-label { font-size: 0.7rem; font-weight: 600; color: var(--text-muted); margin-bottom: 0.5rem; text-transform: uppercase; }
    .theme-btns { display: flex; gap: 0.25rem; }
    .theme-btn { flex: 1; padding: 0.5rem; background: var(--bg); border: 1px solid var(--border); border-radius: 0.5rem; color: var(--text); font-size: 0.75rem; cursor: pointer; }
    .theme-btn.active { background: var(--blue); color: #fff; border-color: var(--blue); }
    .toggle-row { display: flex; align-items: center; justify-content: space-between; padding: 0.4rem 0; }
    .toggle-label { font-size: 0.8rem; }
    .toggle { width: 40px; height: 22px; background: var(--border); border-radius: 11px; position: relative; cursor: pointer; }
    .toggle.active { background: var(--blue); }
    .toggle::after { content: ''; position: absolute; width: 18px; height: 18px; background: #fff; border-radius: 50%; top: 2px; left: 2px; transition: transform 0.2s; }
    .toggle.active::after { transform: translateX(18px); }
    .model-select { margin-top: 0.5rem; }
    .model-grid { display: flex; flex-wrap: wrap; gap: 0.25rem; margin-top: 0.25rem; }
    .model-chip { padding: 0.25rem 0.5rem; background: var(--bg); border: 1px solid var(--border); border-radius: 0.25rem; font-size: 0.7rem; cursor: pointer; }
    .model-chip.selected { background: var(--blue); color: #fff; border-color: var(--blue); }
    .model-dropdown { width: 100%; padding: 0.5rem; border-radius: 0.5rem; border: 1px solid var(--border); background: var(--bg); color: var(--text); font-size: 0.8rem; margin-top: 0.5rem; }
    
    /* Main Content */
    .main-content { flex: 1; display: flex; flex-direction: column; background: var(--bg); min-width: 0; }
    .section-header { padding: 1rem 1.5rem; border-bottom: 1px solid var(--border); }
    .section-header h2 { font-size: 1.125rem; font-weight: 600; }
    
    /* Chat */
    .chat-messages { flex: 1; overflow-y: auto; padding: 1.5rem; display: flex; flex-direction: column; gap: 1rem; }
    .message { max-width: 42rem; padding: 0.75rem 1rem; border-radius: 1rem; line-height: 1.5; }
    .message.user { align-self: flex-end; background: #2563eb; color: #fff; }
    .message.ai { align-self: flex-start; background: var(--bg-secondary); }
    .chat-input-area { padding: 1rem; border-top: 1px solid var(--border); }
    .chat-input-wrapper { max-width: 56rem; margin: 0 auto; display: flex; gap: 0.75rem; }
    .chat-input { flex: 1; padding: 0.75rem 1rem; border-radius: 9999px; border: 1px solid var(--border); background: var(--bg-secondary); color: var(--text); font-size: 1rem; }
    .chat-input:focus { outline: none; border-color: var(--blue); }
    .send-btn { padding: 0.75rem 1.5rem; background: #000; color: #fff; border: none; border-radius: 9999px; cursor: pointer; }
    .dark .send-btn { background: #fff; color: #000; }
    
    /* Sections */
    .section-content { padding: 1.5rem; overflow-y: auto; flex: 1; }
    .history-item { padding: 1rem; background: var(--bg-secondary); border-radius: 0.5rem; margin-bottom: 0.75rem; cursor: pointer; }
    .history-date { font-size: 0.75rem; color: var(--text-muted); }
    .history-preview { margin-top: 0.25rem; font-size: 0.875rem; }
    .chart-container { background: var(--bg-secondary); border-radius: 0.5rem; padding: 1rem; margin-bottom: 1rem; }
    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 1rem; }
    .stat-card { background: var(--bg-secondary); padding: 1rem; border-radius: 0.5rem; text-align: center; }
    .stat-value { font-size: 1.5rem; font-weight: 700; }
    .stat-label { font-size: 0.7rem; color: var(--text-muted); }
    .news-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 1rem; }
    .news-card { background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 1rem; overflow: hidden; cursor: pointer; }
    .news-image { height: 140px; background-size: cover; background-position: center; background-color: var(--border); }
    .news-content { padding: 1rem; }
    .news-title { font-weight: 600; margin-bottom: 0.5rem; font-size: 0.9rem; }
    .news-summary { font-size: 0.8rem; color: var(--text-muted); }
    
    /* Modal */
    .modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,0.5); display: none; align-items: center; justify-content: center; z-index: 100; }
    .modal-overlay.active { display: flex; }
    .modal { background: var(--bg); border: 1px solid var(--border); border-radius: 0.75rem; padding: 1.5rem; width: 90%; max-width: 400px; }
    .modal h3 { margin-bottom: 1rem; }
    .modal-input { width: 100%; padding: 0.75rem; border: 1px solid var(--border); border-radius: 0.5rem; background: var(--bg-secondary); color: var(--text); margin-bottom: 1rem; }
    .modal-btns { display: flex; gap: 0.5rem; justify-content: flex-end; }
    .modal-btn { padding: 0.5rem 1rem; border-radius: 0.5rem; cursor: pointer; border: none; }
    .modal-btn-cancel { background: var(--bg-secondary); color: var(--text); }
    .modal-btn-primary { background: var(--blue); color: #fff; }
    .modal-btn-danger { background: #ef4444; color: #fff; }
  </style>
</head>
<body>
  <!-- Login -->
  <div class="login-page" id="loginPage">
    <div class="login-wrapper">
      <div class="login-header">
        <div class="login-logo">
          <div class="login-logo-icon"><svg viewBox="0 0 24 24" style="width:2rem;height:2rem;"><path d="M3 12l7-9 4 9 7-6v13H3z" fill="black"/></svg></div>
          <h1>Hike.ai</h1>
        </div>
        <p class="login-subtitle">Sign in to continue</p>
      </div>
      <div class="login-card">
        <div id="errorMessage" class="error-message"></div>
        <div id="loginForm">
          <div class="form-group"><label class="form-label">Email</label><input type="email" id="loginEmail" class="form-input" placeholder="Enter your email"></div>
          <div class="form-group"><label class="form-label">Password</label><div class="password-wrapper"><input type="password" id="loginPassword" class="form-input" placeholder="Password" style="padding-right:4rem;"><button type="button" class="password-toggle" onclick="togglePassword('loginPassword',this)">Show</button></div></div>
          <button class="btn btn-primary" id="loginBtn">Sign In</button>
          <div class="divider"><div class="divider-line"></div><span class="divider-text">or</span></div>
          <button class="btn btn-google" id="googleBtn"><svg width="18" height="18" viewBox="0 0 24 24"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg>Google</button>
          <div class="login-footer">No account? <a onclick="showSignup()">Sign up</a></div>
        </div>
        <div id="signupForm" class="hidden">
          <div class="form-group"><label class="form-label">Name</label><input type="text" id="signupName" class="form-input" placeholder="Your name"></div>
          <div class="form-group"><label class="form-label">Email</label><input type="email" id="signupEmail" class="form-input" placeholder="Email"></div>
          <div class="form-group"><label class="form-label">Password</label><div class="password-wrapper"><input type="password" id="signupPassword" class="form-input" placeholder="Password" style="padding-right:4rem;"><button type="button" class="password-toggle" onclick="togglePassword('signupPassword',this)">Show</button></div></div>
          <button class="btn btn-primary" id="signupBtn">Create Account</button>
          <div class="login-footer">Have account? <a onclick="showLogin()">Sign in</a></div>
        </div>
      </div>
    </div>
  </div>

  <!-- App -->
  <div class="app" id="mainApp">
    <aside class="sidebar">
      <div class="sidebar-header">
        <div class="sidebar-logo">
          <div class="sidebar-logo-icon"><svg viewBox="0 0 24 24" fill="currentColor" style="width:1.5rem;height:1.5rem;"><path d="M3 12l7-9 4 9 7-6v13H3z"/></svg></div>
          <h1>Hike.ai</h1>
        </div>
      </div>
      <nav class="sidebar-nav">
        <button class="nav-item active" id="navChat" onclick="showSection('chat')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>Chat</button>
        <button class="nav-item" id="navHistory" onclick="showSection('history')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>History</button>
        <button class="nav-item" id="navProjects" onclick="toggleProjects()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>Projects<svg id="projectsChevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:1rem;height:1rem;margin-left:auto;"><path d="M9 18l6-6-6-6"/></svg></button>
        <div id="projectsList" class="projects-list hidden">
          <div class="project-item create-project" onclick="openCreateProject()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:1rem;height:1rem;margin-right:0.5rem;"><path d="M12 5v14M5 12h14"/></svg>Create Project</div>
        </div>
        <button class="nav-item" id="navNews" onclick="showSection('news')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M19 20H5a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v1m2 13a2 2 0 0 1-2-2V7m2 13a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2h-2"/></svg>News</button>
        <button class="nav-item" id="navAnalysis" onclick="showSection('analysis')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 20V10M12 20V4M6 20v-6"/></svg>Analysis</button>
      </nav>
      <div class="sidebar-footer">
        <button class="nav-item" onclick="toggleSettings()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M12 1v2m0 18v2M4.22 4.22l1.42 1.42m12.72 12.72l1.42 1.42M1 12h2m18 0h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/></svg>Settings</button>
        <div id="settingsPanel" class="settings-panel hidden">
          <div class="settings-section">
            <div class="settings-label">Theme</div>
            <div class="theme-btns">
              <button class="theme-btn" onclick="setTheme('light')">Light</button>
              <button class="theme-btn active" onclick="setTheme('dark')">Dark</button>
              <button class="theme-btn" onclick="setTheme('system')">System</button>
            </div>
          </div>
          <div class="settings-section">
            <div class="settings-label">Default Model</div>
            <select class="model-dropdown" id="defaultModel" onchange="setDefaultModel(this.value)">
              <option value="auto">Auto</option>
              <option value="groq">Groq</option>
              <option value="openrouter">OpenRouter</option>
              <option value="gemini">Gemini</option>
              <option value="bytez">Bytez</option>
              <option value="chutes">Chutes</option>
            </select>
          </div>
          <div class="settings-section">
            <div class="toggle-row"><span class="toggle-label">Web Search</span><div class="toggle" id="webSearchToggle" onclick="toggleSetting('webSearch')"></div></div>
          </div>
          <div class="settings-section">
            <div class="toggle-row"><span class="toggle-label">Debate AI</span><div class="toggle" id="debateToggle" onclick="toggleSetting('debate')"></div></div>
            <div class="model-select hidden" id="debateModels">
              <div class="settings-label">Select 2-4 models</div>
              <div class="model-grid">
                <span class="model-chip selected" data-model="groq" onclick="toggleModel('debate','groq')">Groq</span>
                <span class="model-chip selected" data-model="openrouter" onclick="toggleModel('debate','openrouter')">OpenRouter</span>
                <span class="model-chip" data-model="gemini" onclick="toggleModel('debate','gemini')">Gemini</span>
                <span class="model-chip" data-model="bytez" onclick="toggleModel('debate','bytez')">Bytez</span>
              </div>
            </div>
          </div>
          <div class="settings-section">
            <div class="toggle-row"><span class="toggle-label">Regret AI</span><div class="toggle" id="regretToggle" onclick="toggleSetting('regret')"></div></div>
            <div class="model-select hidden" id="regretModels">
              <div class="settings-label">Select 2-4 models</div>
              <div class="model-grid">
                <span class="model-chip selected" data-model="groq" onclick="toggleModel('regret','groq')">Groq</span>
                <span class="model-chip selected" data-model="openrouter" onclick="toggleModel('regret','openrouter')">OpenRouter</span>
                <span class="model-chip" data-model="gemini" onclick="toggleModel('regret','gemini')">Gemini</span>
                <span class="model-chip" data-model="chutes" onclick="toggleModel('regret','chutes')">Chutes</span>
              </div>
            </div>
          </div>
          <div class="settings-section">
            <div class="settings-label">Empathy AI Model</div>
            <select class="model-dropdown" id="empathyModel" onchange="setEmpathyModel(this.value)">
              <option value="groq">Groq</option>
              <option value="openrouter">OpenRouter</option>
              <option value="gemini">Gemini</option>
              <option value="bytez">Bytez</option>
              <option value="chutes">Chutes</option>
            </select>
          </div>
        </div>
        <button class="nav-item" style="margin-top:0.5rem;color:#ef4444;" onclick="handleLogout()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4M16 17l5-5-5-5M21 12H9"/></svg>Logout</button>
      </div>
    </aside>
    <main class="main-content">
      <div id="chatSection" style="display:flex;flex-direction:column;height:100%;">
        <div class="section-header"><h2>Chat</h2></div>
        <div class="chat-messages" id="chatMessages"><div class="message ai">Hello! I'm Hike.ai. How can I help?</div></div>
        <div class="chat-input-area"><div class="chat-input-wrapper"><input type="text" id="chatInput" class="chat-input" placeholder="Type a message..."><button class="send-btn" onclick="sendMessage()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:1.25rem;height:1.25rem;"><path d="M22 2L11 13M22 2l-7 20-4-9-9-4 20-7z"/></svg></button></div></div>
      </div>
      <div id="historySection" class="hidden" style="display:flex;flex-direction:column;height:100%;">
        <div class="section-header"><h2>History</h2></div>
        <div class="section-content" id="historyList"><p style="color:var(--text-muted);text-align:center;margin-top:2rem;">No history yet.</p></div>
      </div>
      <div id="newsSection" class="hidden" style="display:flex;flex-direction:column;height:100%;">
        <div class="section-header"><h2>News</h2></div>
        <div class="section-content" id="newsGrid"><p style="color:var(--text-muted);">Loading...</p></div>
      </div>
      <div id="analysisSection" class="hidden" style="display:flex;flex-direction:column;height:100%;">
        <div class="section-header"><h2>Analysis</h2></div>
        <div class="section-content">
          <div class="chart-container"><canvas id="usageChart" height="200"></canvas></div>
          <div class="stats-grid">
            <div class="stat-card"><div class="stat-value" id="statTotal">0</div><div class="stat-label">Total</div></div>
            <div class="stat-card"><div class="stat-value" id="statGroq">0</div><div class="stat-label">Groq</div></div>
            <div class="stat-card"><div class="stat-value" id="statOpenRouter">0</div><div class="stat-label">OpenRouter</div></div>
            <div class="stat-card"><div class="stat-value" id="statGemini">0</div><div class="stat-label">Gemini</div></div>
          </div>
        </div>
      </div>
    </main>
  </div>

  <!-- Create Project Modal -->
  <div class="modal-overlay" id="createProjectModal">
    <div class="modal">
      <h3>Create Project</h3>
      <input type="text" class="modal-input" id="newProjectName" placeholder="Project name...">
      <div class="modal-btns">
        <button class="modal-btn modal-btn-cancel" onclick="closeCreateProject()">Cancel</button>
        <button class="modal-btn modal-btn-primary" onclick="createProject()">Create</button>
      </div>
    </div>
  </div>

  <!-- Delete Project Modal -->
  <div class="modal-overlay" id="deleteProjectModal">
    <div class="modal">
      <h3>Delete Project</h3>
      <p style="margin-bottom:1rem;color:var(--text-muted);">Delete "<span id="deleteProjectName"></span>"?</p>
      <div class="modal-btns">
        <button class="modal-btn modal-btn-cancel" onclick="closeDeleteProject()">Cancel</button>
        <button class="modal-btn modal-btn-danger" onclick="confirmDeleteProject()">Delete</button>
      </div>
    </div>
  </div>

  <script>
    const state = {
      theme: localStorage.getItem('theme') || 'dark',
      defaultModel: localStorage.getItem('defaultModel') || 'auto',
      empathyModel: localStorage.getItem('empathyModel') || 'groq',
      webSearch: localStorage.getItem('webSearch') === 'true',
      debate: localStorage.getItem('debate') === 'true',
      regret: localStorage.getItem('regret') === 'true',
      debateModels: JSON.parse(localStorage.getItem('debateModels') || '["groq","openrouter"]'),
      regretModels: JSON.parse(localStorage.getItem('regretModels') || '["groq","openrouter"]'),
      projects: JSON.parse(localStorage.getItem('projects') || '[]'),
      chatHistory: JSON.parse(localStorage.getItem('chatHistory') || '[]'),
      modelUsage: JSON.parse(localStorage.getItem('modelUsage') || '{"groq":0,"openrouter":0,"gemini":0,"bytez":0,"chutes":0}'),
      usageHistory: JSON.parse(localStorage.getItem('usageHistory') || '[]'),
      projectToDelete: null
    };
    let usageChart = null;

    document.addEventListener('DOMContentLoaded', () => { applyTheme(); applySettings(); checkSession(); bindEvents(); });

    function bindEvents() {
      document.getElementById('loginBtn').addEventListener('click', handleLogin);
      document.getElementById('signupBtn').addEventListener('click', handleSignup);
      document.getElementById('googleBtn').addEventListener('click', () => window.location.href='/auth/google/login');
      document.getElementById('chatInput').addEventListener('keypress', e => { if(e.key==='Enter') sendMessage(); });
      document.getElementById('loginPassword').addEventListener('keypress', e => { if(e.key==='Enter') handleLogin(); });
      document.getElementById('newProjectName').addEventListener('keypress', e => { if(e.key==='Enter') createProject(); });
    }

    async function checkSession() { try { const r = await fetch('/api/profile'); if(r.ok) showApp(); } catch(e){} }
    async function handleLogin() {
      const email = document.getElementById('loginEmail').value, password = document.getElementById('loginPassword').value;
      if(!email||!password) { showError('Enter email and password'); return; }
      try { const r = await fetch('/api/login', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({email,password}) }); if(r.ok) showApp(); else { const d=await r.json(); showError(d.detail||'Failed'); } } catch(e) { showError('Error'); }
    }
    async function handleSignup() {
      const name=document.getElementById('signupName').value, email=document.getElementById('signupEmail').value, password=document.getElementById('signupPassword').value;
      if(!name||!email||!password) { showError('Fill all fields'); return; }
      try { const r = await fetch('/api/signup', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({name,email,password}) }); if(r.ok) showApp(); else { const d=await r.json(); showError(d.detail||'Failed'); } } catch(e) { showError('Error'); }
    }
    async function handleLogout() { try { await fetch('/api/logout',{method:'POST'}); } catch(e){} document.getElementById('mainApp').classList.remove('active'); document.getElementById('loginPage').style.display='flex'; }

    function showApp() { document.getElementById('loginPage').style.display='none'; document.getElementById('mainApp').classList.add('active'); loadNews(); renderHistory(); renderProjects(); initChart(); }
    function showError(msg) { const el=document.getElementById('errorMessage'); el.textContent=msg; el.classList.add('show'); setTimeout(()=>el.classList.remove('show'),5000); }
    function showLogin() { document.getElementById('signupForm').classList.add('hidden'); document.getElementById('loginForm').classList.remove('hidden'); }
    function showSignup() { document.getElementById('loginForm').classList.add('hidden'); document.getElementById('signupForm').classList.remove('hidden'); }
    function togglePassword(id,btn) { const i=document.getElementById(id); i.type=i.type==='password'?'text':'password'; btn.textContent=i.type==='password'?'Show':'Hide'; }

    function showSection(section) {
      ['chat','history','news','analysis'].forEach(s => { document.getElementById(s+'Section').classList.add('hidden'); document.getElementById('nav'+s.charAt(0).toUpperCase()+s.slice(1))?.classList.remove('active'); });
      document.getElementById(section+'Section').classList.remove('hidden');
      document.getElementById(section+'Section').style.display='flex';
      document.getElementById('nav'+section.charAt(0).toUpperCase()+section.slice(1))?.classList.add('active');
      if(section==='news') loadNews();
      if(section==='history') renderHistory();
      if(section==='analysis') updateChart();
    }

    async function sendMessage() {
      const input=document.getElementById('chatInput'), msg=input.value.trim();
      if(!msg) return;
      const chat=document.getElementById('chatMessages');
      chat.innerHTML += '<div class="message user">'+msg+'</div>';
      input.value = '';
      const aiMsg=document.createElement('div'); aiMsg.className='message ai'; aiMsg.textContent='Thinking...'; chat.appendChild(aiMsg); chat.scrollTop=chat.scrollHeight;
      try {
        const r = await fetch('/api/chat', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({message:msg,selected_model:state.defaultModel,empathy_model:state.empathyModel,use_research:state.webSearch,use_debate:state.debate,debate_models:state.debateModels,use_regret:state.regret,regret_models:state.regretModels}) });
        const d = await r.json();
        aiMsg.textContent = d.response || 'Sorry, error occurred.';
        saveToHistory(msg, d.response);
        if(d.model_used) trackModelUsage(d.model_used);
      } catch(e) { aiMsg.textContent = 'Error. Try again.'; }
      chat.scrollTop = chat.scrollHeight;
    }

    function saveToHistory(user, ai) { state.chatHistory.unshift({ date: new Date().toISOString(), user, ai }); if(state.chatHistory.length > 50) state.chatHistory.pop(); localStorage.setItem('chatHistory', JSON.stringify(state.chatHistory)); }
    function renderHistory() {
      const list = document.getElementById('historyList');
      if(state.chatHistory.length === 0) { list.innerHTML = '<p style="color:var(--text-muted);text-align:center;margin-top:2rem;">No history yet.</p>'; return; }
      list.innerHTML = state.chatHistory.map(h => '<div class="history-item"><div class="history-date">'+new Date(h.date).toLocaleString()+'</div><div class="history-preview"><strong>You:</strong> '+h.user.substring(0,50)+'...</div></div>').join('');
    }

    function trackModelUsage(model) { const m=model.toLowerCase(); if(state.modelUsage[m]!==undefined) state.modelUsage[m]++; state.usageHistory.push({date:new Date().toISOString(),model:m}); if(state.usageHistory.length>100) state.usageHistory.shift(); localStorage.setItem('modelUsage',JSON.stringify(state.modelUsage)); localStorage.setItem('usageHistory',JSON.stringify(state.usageHistory)); }

    function initChart() {
      const ctx = document.getElementById('usageChart').getContext('2d');
      usageChart = new Chart(ctx, { type:'line', data:{labels:[],datasets:[{label:'Groq',data:[],borderColor:'#10b981',fill:false},{label:'OpenRouter',data:[],borderColor:'#3b82f6',fill:false},{label:'Gemini',data:[],borderColor:'#f59e0b',fill:false}]}, options:{responsive:true,scales:{y:{beginAtZero:true}}} });
      updateChart();
    }
    function updateChart() {
      if(!usageChart) return;
      const last7=[...Array(7)].map((_,i)=>{const d=new Date();d.setDate(d.getDate()-6+i);return d.toLocaleDateString('en-US',{month:'short',day:'numeric'});});
      const counts={groq:Array(7).fill(0),openrouter:Array(7).fill(0),gemini:Array(7).fill(0)};
      state.usageHistory.forEach(u=>{const d=new Date(u.date).toLocaleDateString('en-US',{month:'short',day:'numeric'});const idx=last7.indexOf(d);if(idx>=0&&counts[u.model])counts[u.model][idx]++;});
      usageChart.data.labels=last7; usageChart.data.datasets[0].data=counts.groq; usageChart.data.datasets[1].data=counts.openrouter; usageChart.data.datasets[2].data=counts.gemini; usageChart.update();
      document.getElementById('statTotal').textContent=Object.values(state.modelUsage).reduce((a,b)=>a+b,0);
      document.getElementById('statGroq').textContent=state.modelUsage.groq||0;
      document.getElementById('statOpenRouter').textContent=state.modelUsage.openrouter||0;
      document.getElementById('statGemini').textContent=state.modelUsage.gemini||0;
    }

    async function loadNews() {
      const grid=document.getElementById('newsGrid');
      try { const r=await fetch('/api/news/latest'), articles=await r.json(); if(!articles||articles.length===0){grid.innerHTML='<p>No news.</p>';return;} grid.innerHTML='<div class="news-grid">'+articles.map(a=>'<div class="news-card" onclick="window.open(\''+a.url+'\',\'_blank\')"><div class="news-image" style="'+(a.urlToImage?'background-image:url(\''+a.urlToImage+'\')':'background:#374151')+'"></div><div class="news-content"><div class="news-title">'+a.title+'</div><div class="news-summary">'+(a.description||'')+'</div></div></div>').join('')+'</div>'; } catch(e){grid.innerHTML='<p style="color:#ef4444;">Failed.</p>';}
    }

    // Projects
    function toggleProjects() { const list=document.getElementById('projectsList'),chev=document.getElementById('projectsChevron'); list.classList.toggle('hidden'); chev.style.transform=list.classList.contains('hidden')?'':'rotate(90deg)'; }
    function renderProjects() {
      const list=document.getElementById('projectsList'), createBtn=list.querySelector('.create-project');
      list.innerHTML=''; list.appendChild(createBtn);
      state.projects.forEach(p=>{const item=document.createElement('div');item.className='project-item';item.innerHTML='<span class="project-name">'+p+'</span><button class="project-delete" onclick="openDeleteProject(\''+p+'\',event)"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:1rem;height:1rem;"><path d="M3 6h18M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg></button>';list.appendChild(item);});
    }
    function openCreateProject() { document.getElementById('createProjectModal').classList.add('active'); document.getElementById('newProjectName').focus(); }
    function closeCreateProject() { document.getElementById('createProjectModal').classList.remove('active'); document.getElementById('newProjectName').value=''; }
    function createProject() { const name=document.getElementById('newProjectName').value.trim(); if(name){state.projects.push(name);localStorage.setItem('projects',JSON.stringify(state.projects));renderProjects();closeCreateProject();} }
    function openDeleteProject(name,e) { e.stopPropagation(); state.projectToDelete=name; document.getElementById('deleteProjectName').textContent=name; document.getElementById('deleteProjectModal').classList.add('active'); }
    function closeDeleteProject() { document.getElementById('deleteProjectModal').classList.remove('active'); state.projectToDelete=null; }
    function confirmDeleteProject() { if(state.projectToDelete){state.projects=state.projects.filter(p=>p!==state.projectToDelete);localStorage.setItem('projects',JSON.stringify(state.projects));renderProjects();closeDeleteProject();} }

    // Settings
    function toggleSettings() { document.getElementById('settingsPanel').classList.toggle('hidden'); }
    function setTheme(t) { state.theme=t; localStorage.setItem('theme',t); applyTheme(); document.querySelectorAll('.theme-btn').forEach(b=>b.classList.remove('active')); event.target.classList.add('active'); }
    function applyTheme() { document.body.classList.toggle('dark', state.theme==='dark'||(state.theme==='system'&&window.matchMedia('(prefers-color-scheme:dark)').matches)); }
    function setDefaultModel(m) { state.defaultModel=m; localStorage.setItem('defaultModel',m); }
    function setEmpathyModel(m) { state.empathyModel=m; localStorage.setItem('empathyModel',m); }
    function applySettings() {
      document.getElementById('defaultModel').value=state.defaultModel;
      document.getElementById('empathyModel').value=state.empathyModel;
      if(state.webSearch) document.getElementById('webSearchToggle').classList.add('active');
      if(state.debate) { document.getElementById('debateToggle').classList.add('active'); document.getElementById('debateModels').classList.remove('hidden'); }
      if(state.regret) { document.getElementById('regretToggle').classList.add('active'); document.getElementById('regretModels').classList.remove('hidden'); }
      updateModelChips();
    }
    function toggleSetting(s) { state[s]=!state[s]; localStorage.setItem(s,state[s]); document.getElementById(s+'Toggle').classList.toggle('active',state[s]); if(s==='debate'||s==='regret') document.getElementById(s+'Models').classList.toggle('hidden',!state[s]); }
    function toggleModel(type,model) { const arr=state[type+'Models'],idx=arr.indexOf(model); if(idx>=0){if(arr.length>2)arr.splice(idx,1);} else{if(arr.length<4)arr.push(model);} localStorage.setItem(type+'Models',JSON.stringify(arr)); updateModelChips(); }
    function updateModelChips() { document.querySelectorAll('#debateModels .model-chip').forEach(c=>c.classList.toggle('selected',state.debateModels.includes(c.dataset.model))); document.querySelectorAll('#regretModels .model-chip').forEach(c=>c.classList.toggle('selected',state.regretModels.includes(c.dataset.model))); }
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

@app.get("/.well-known/{path:path}", include_in_schema=False)
async def wellknown(path: str):
    return Response(content="{}", media_type="application/json")

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