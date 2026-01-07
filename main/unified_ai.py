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

# Placeholder for combined HTML
HTML_CONTENT = r"""
<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Hike.ai</title>

  <style>
    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #fafafa;
      transition: background 0.3s ease;
    }

    body.dark {
      background: #0a0a0a;
    }

    /* Login Page */
    .login-page {
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 100vh;
      padding: 20px;
    }

    .login-container {
      width: 100%;
      max-width: 400px;
      background: #fff;
      border-radius: 20px;
      padding: 48px 40px;
      box-shadow: 0 4px 24px rgba(0, 0, 0, 0.06);
    }

    body.dark .login-container {
      background: #111;
      box-shadow: 0 4px 24px rgba(0, 0, 0, 0.3);
    }

    .login-logo {
      font-size: 28px;
      font-weight: 600;
      letter-spacing: -0.02em;
      color: #0a0a0a;
      margin-bottom: 12px;
      text-align: center;
    }

    body.dark .login-logo {
      color: #fafafa;
    }

    .login-subtitle {
      text-align: center;
      color: #666;
      font-size: 14px;
      margin-bottom: 32px;
    }

    body.dark .login-subtitle {
      color: #888;
    }

    .form-group {
      margin-bottom: 16px;
    }

    .form-label {
      display: block;
      font-size: 13px;
      color: #666;
      margin-bottom: 8px;
    }

    body.dark .form-label {
      color: #888;
    }

    .form-input {
      width: 100%;
      padding: 14px 16px;
      border-radius: 12px;
      border: 1px solid #e8e8e8;
      font-size: 14px;
      font-family: inherit;
      background: #fafafa;
      color: #0a0a0a;
      transition: all 0.2s ease;
    }

    body.dark .form-input {
      background: #1a1a1a;
      border-color: #222;
      color: #fafafa;
    }

    .form-input:focus {
      outline: none;
      border-color: #999;
    }

    .form-input::placeholder {
      color: #999;
    }

    .login-btn {
      width: 100%;
      padding: 14px;
      border-radius: 12px;
      border: none;
      background: #0a0a0a;
      color: #fff;
      font-size: 14px;
      font-weight: 500;
      cursor: pointer;
      transition: all 0.2s ease;
      margin-top: 24px;
    }

    body.dark .login-btn {
      background: #fafafa;
      color: #0a0a0a;
    }

    .login-btn:hover {
      opacity: 0.9;
      transform: translateY(-1px);
    }

    .login-btn:active {
      transform: translateY(0);
    }

    .login-btn:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }

    .divider {
      display: flex;
      align-items: center;
      margin: 24px 0;
      color: #999;
      font-size: 13px;
    }

    .divider::before,
    .divider::after {
      content: '';
      flex: 1;
      height: 1px;
      background: #e8e8e8;
    }

    body.dark .divider::before,
    body.dark .divider::after {
      background: #222;
    }

    .divider span {
      padding: 0 16px;
    }

    .google-btn {
      width: 100%;
      padding: 14px;
      border-radius: 12px;
      border: 1px solid #e8e8e8;
      background: #fff;
      color: #0a0a0a;
      font-size: 14px;
      font-weight: 500;
      cursor: pointer;
      transition: all 0.2s ease;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 10px;
    }

    body.dark .google-btn {
      background: #1a1a1a;
      border-color: #222;
      color: #fafafa;
    }

    .google-btn:hover {
      background: #fafafa;
      border-color: #ccc;
    }

    body.dark .google-btn:hover {
      background: #222;
      border-color: #333;
    }

    .switch-mode {
      text-align: center;
      margin-top: 24px;
      font-size: 13px;
      color: #666;
    }

    body.dark .switch-mode {
      color: #888;
    }

    .switch-mode a {
      color: #0a0a0a;
      font-weight: 500;
      cursor: pointer;
      text-decoration: none;
    }

    body.dark .switch-mode a {
      color: #fafafa;
    }

    .switch-mode a:hover {
      text-decoration: underline;
    }

    .theme-toggle-login {
      position: absolute;
      top: 20px;
      right: 20px;
      width: 40px;
      height: 40px;
      border-radius: 10px;
      background: #fff;
      border: 1px solid #e8e8e8;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 12px;
      font-weight: 500;
      transition: all 0.2s ease;
      color: #666;
    }

    body.dark .theme-toggle-login {
      background: #111;
      border-color: #222;
      color: #888;
    }

    .theme-toggle-login:hover {
      background: #f5f5f5;
    }

    body.dark .theme-toggle-login:hover {
      background: #1a1a1a;
    }

    .error-msg {
      background: rgba(239, 68, 68, 0.1);
      color: #ef4444;
      padding: 12px 16px;
      border-radius: 12px;
      font-size: 13px;
      margin-bottom: 16px;
      display: none;
    }

    .error-msg.show {
      display: block;
    }

    /* Main App */
    .app {
      display: none;
      height: 100vh;
    }

    .app.active {
      display: flex;
    }

    /* Sidebar */
    .sidebar {
      width: 260px;
      background: #fff;
      border-right: 1px solid #e8e8e8;
      padding: 24px 16px;
      display: flex;
      flex-direction: column;
      gap: 32px;
    }

    body.dark .sidebar {
      background: #111;
      border-color: #222;
    }

    .logo {
      font-size: 18px;
      font-weight: 600;
      letter-spacing: -0.02em;
      color: #0a0a0a;
      padding: 0 12px;
    }

    body.dark .logo {
      color: #fafafa;
    }

    nav {
      display: flex;
      flex-direction: column;
      gap: 2px;
    }

    nav a {
      padding: 12px;
      border-radius: 10px;
      cursor: pointer;
      font-size: 14px;
      color: #666;
      transition: all 0.2s ease;
      text-decoration: none;
    }

    body.dark nav a {
      color: #888;
    }

    nav a.active {
      background: #f5f5f5;
      color: #0a0a0a;
    }

    body.dark nav a.active {
      background: #1a1a1a;
      color: #fafafa;
    }

    nav a:hover {
      background: #f5f5f5;
      color: #0a0a0a;
    }

    body.dark nav a:hover {
      background: #1a1a1a;
      color: #fafafa;
    }

    /* Sidebar Bottom Settings */
    .sidebar-bottom {
      margin-top: auto;
    }

    .settings-sidebar-btn {
      width: 100%;
      padding: 12px 14px;
      border-radius: 10px;
      background: #f5f5f5;
      border: 1px solid #e8e8e8;
      color: #666;
      font-size: 14px;
      font-family: inherit;
      cursor: pointer;
      transition: all 0.2s ease;
      display: flex;
      align-items: center;
      gap: 10px;
    }

    body.dark .settings-sidebar-btn {
      background: #1a1a1a;
      border-color: #333;
      color: #888;
    }

    .settings-sidebar-btn:hover {
      background: #e8e8e8;
      color: #0a0a0a;
    }

    body.dark .settings-sidebar-btn:hover {
      background: #222;
      color: #fafafa;
    }

    .settings-icon {
      font-size: 16px;
      transition: transform 0.3s ease;
    }

    .settings-sidebar-btn:hover .settings-icon {
      transform: rotate(90deg);
    }

    /* Chat */
    .chat {
      flex: 1;
      display: flex;
      flex-direction: column;
      background: #fff;
    }

    body.dark .chat {
      background: #111;
    }

    .chat-header {
      padding: 20px 32px;
      border-bottom: 1px solid #e8e8e8;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    body.dark .chat-header {
      border-color: #222;
    }

    .chat-header strong {
      font-size: 15px;
      font-weight: 600;
      color: #0a0a0a;
    }

    body.dark .chat-header strong {
      color: #fafafa;
    }

    .chat-header span {
      font-size: 13px;
      color: #999;
    }

    .chat-body {
      flex: 1;
      padding: 32px;
      overflow-y: auto;
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .message {
      max-width: 65%;
      padding: 16px 20px;
      border-radius: 16px;
      font-size: 14px;
      line-height: 1.6;
      white-space: pre-wrap;
    }

    .message.ai {
      background: #f5f5f5;
      color: #0a0a0a;
      align-self: flex-start;
    }

    body.dark .message.ai {
      background: #1a1a1a;
      color: #e8e8e8;
    }

    .message.user {
      background: #0a0a0a;
      color: #fff;
      align-self: flex-end;
    }

    body.dark .message.user {
      background: #fafafa;
      color: #0a0a0a;
    }

    /* Agent Panel */
    .agent-panel {
      border-top: 1px solid #e8e8e8;
      padding: 20px 32px;
      background: #fafafa;
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 20px;
      font-size: 13px;
    }

    body.dark .agent-panel {
      background: #0a0a0a;
      border-color: #222;
    }

    .agent-section {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    .agent {
      display: flex;
      justify-content: space-between;
      align-items: center;
      color: #666;
    }

    body.dark .agent {
      color: #888;
    }

    .agent span {
      color: #999;
      font-size: 12px;
    }

    /* Metrics */
    .metric {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }

    .metric-label {
      color: #666;
      font-size: 12px;
    }

    body.dark .metric-label {
      color: #888;
    }

    .bar {
      height: 4px;
      border-radius: 4px;
      background: #e8e8e8;
      overflow: hidden;
    }

    body.dark .bar {
      background: #222;
    }

    .fill {
      height: 100%;
      background: #0a0a0a;
      transition: width 0.3s ease;
    }

    body.dark .fill {
      background: #fafafa;
    }

    /* Input */
    .chat-input {
      display: flex;
      gap: 12px;
      padding: 20px 32px;
      border-top: 1px solid #e8e8e8;
      background: #fff;
    }

    body.dark .chat-input {
      background: #111;
      border-color: #222;
    }

    textarea {
      flex: 1;
      resize: none;
      padding: 14px 16px;
      border-radius: 12px;
      border: 1px solid #e8e8e8;
      font-size: 14px;
      font-family: inherit;
      background: #fafafa;
      color: #0a0a0a;
      transition: all 0.2s ease;
    }

    body.dark textarea {
      background: #1a1a1a;
      border-color: #222;
      color: #fafafa;
    }

    textarea:focus {
      outline: none;
      border-color: #999;
    }

    textarea::placeholder {
      color: #999;
    }

    button {
      width: 48px;
      height: 48px;
      border-radius: 12px;
      border: none;
      background: #0a0a0a;
      color: #fff;
      font-size: 18px;
      cursor: pointer;
      transition: all 0.2s ease;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    body.dark button {
      background: #fafafa;
      color: #0a0a0a;
    }

    button:hover {
      opacity: 0.9;
      transform: scale(0.98);
    }

    button:active {
      transform: scale(0.95);
    }

    button:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }

    .hidden {
      display: none;
    }

    .password-toggle {
      position: absolute;
      right: 14px;
      top: 50%;
      transform: translateY(-50%);
      cursor: pointer;
      font-size: 11px;
      font-weight: 500;
      user-select: none;
      color: #666;
      transition: color 0.2s ease;
    }

    body.dark .password-toggle {
      color: #888;
    }

    .password-toggle:hover {
      color: #0a0a0a;
    }

    body.dark .password-toggle:hover {
      color: #fafafa;
    }

    /* Settings Button */
    .settings-btn {
      width: 40px;
      height: 40px;
      border-radius: 10px;
      background: #f5f5f5;
      border: 1px solid #e8e8e8;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 18px;
      transition: all 0.2s ease;
      color: #666;
    }

    body.dark .settings-btn {
      background: #1a1a1a;
      border-color: #333;
      color: #888;
    }

    .settings-btn:hover {
      background: #e8e8e8;
      transform: rotate(30deg);
    }

    body.dark .settings-btn:hover {
      background: #222;
    }

    /* Settings Modal Overlay */
    .settings-overlay {
      position: fixed;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      background: rgba(0, 0, 0, 0.5);
      backdrop-filter: blur(4px);
      z-index: 1000;
      display: flex;
      align-items: center;
      justify-content: center;
      opacity: 0;
      visibility: hidden;
      transition: all 0.3s ease;
    }

    .settings-overlay.active {
      opacity: 1;
      visibility: visible;
    }

    /* Settings Panel */
    .settings-panel {
      background: #fff;
      border-radius: 20px;
      padding: 32px;
      width: 100%;
      max-width: 420px;
      max-height: 80vh;
      overflow-y: auto;
      box-shadow: 0 24px 48px rgba(0, 0, 0, 0.15);
      transform: scale(0.95) translateY(20px);
      transition: all 0.3s ease;
    }

    .settings-overlay.active .settings-panel {
      transform: scale(1) translateY(0);
    }

    body.dark .settings-panel {
      background: #111;
      box-shadow: 0 24px 48px rgba(0, 0, 0, 0.5);
    }

    .settings-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 28px;
    }

    .settings-title {
      font-size: 18px;
      font-weight: 600;
      color: #0a0a0a;
    }

    body.dark .settings-title {
      color: #fafafa;
    }

    .settings-close {
      width: 32px;
      height: 32px;
      border-radius: 8px;
      background: #f5f5f5;
      border: none;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 14px;
      color: #666;
      transition: all 0.2s ease;
    }

    body.dark .settings-close {
      background: #1a1a1a;
      color: #888;
    }

    .settings-close:hover {
      background: #e8e8e8;
      color: #0a0a0a;
    }

    body.dark .settings-close:hover {
      background: #222;
      color: #fafafa;
    }

    /* Settings Section */
    .settings-section {
      margin-bottom: 24px;
      padding-bottom: 24px;
      border-bottom: 1px solid #e8e8e8;
    }

    .settings-section:last-child {
      margin-bottom: 0;
      padding-bottom: 0;
      border-bottom: none;
    }

    body.dark .settings-section {
      border-color: #222;
    }

    .settings-label {
      font-size: 13px;
      font-weight: 500;
      color: #0a0a0a;
      margin-bottom: 6px;
      display: block;
    }

    body.dark .settings-label {
      color: #fafafa;
    }

    .settings-description {
      font-size: 12px;
      color: #888;
      margin-bottom: 12px;
    }

    /* Toggle Switch */
    .toggle-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .toggle-switch {
      position: relative;
      width: 48px;
      height: 26px;
      cursor: pointer;
    }

    .toggle-switch input {
      opacity: 0;
      width: 0;
      height: 0;
    }

    .toggle-slider {
      position: absolute;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      background: #e8e8e8;
      border-radius: 26px;
      transition: all 0.3s ease;
    }

    body.dark .toggle-slider {
      background: #333;
    }

    .toggle-slider:before {
      content: '';
      position: absolute;
      width: 20px;
      height: 20px;
      left: 3px;
      bottom: 3px;
      background: #fff;
      border-radius: 50%;
      transition: all 0.3s ease;
      box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }

    .toggle-switch input:checked+.toggle-slider {
      background: #0a0a0a;
    }

    body.dark .toggle-switch input:checked+.toggle-slider {
      background: #fafafa;
    }

    .toggle-switch input:checked+.toggle-slider:before {
      transform: translateX(22px);
    }

    body.dark .toggle-switch input:checked+.toggle-slider:before {
      background: #111;
    }

    /* Model Selector */
    .model-selector {
      width: 100%;
      padding: 12px 16px;
      border-radius: 10px;
      border: 1px solid #e8e8e8;
      background: #fafafa;
      color: #0a0a0a;
      font-size: 14px;
      font-family: inherit;
      cursor: pointer;
      appearance: none;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath fill='%23666' d='M6 8L1 3h10z'/%3E%3C/svg%3E");
      background-repeat: no-repeat;
      background-position: right 14px center;
      padding-right: 40px;
      transition: all 0.2s ease;
    }

    body.dark .model-selector {
      background-color: #1a1a1a;
      border-color: #333;
      color: #fafafa;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath fill='%23fafafa' d='M6 8L1 3h10z'/%3E%3C/svg%3E");
    }

    .model-selector:focus {
      outline: none;
      border-color: #999;
    }

    .model-selector:hover {
      border-color: #ccc;
    }

    body.dark .model-selector:hover {
      border-color: #444;
    }

    .setting-status {
      font-size: 11px;
      padding: 4px 8px;
      border-radius: 6px;
      background: #e8f5e9;
      color: #2e7d32;
      margin-left: 10px;
    }

    body.dark .setting-status {
      background: #1b3320;
      color: #66bb6a;
    }

    .setting-status.inactive {
      background: #fafafa;
      color: #999;
    }

    body.dark .setting-status.inactive {
      background: #222;
      color: #666;
    }

    /* Model Checkbox Grid */
    .model-checkboxes {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
      margin-top: 12px;
      padding: 12px;
      background: #f5f5f5;
      border-radius: 10px;
      max-height: 0;
      overflow: hidden;
      opacity: 0;
      transition: all 0.3s ease;
    }

    .model-checkboxes.visible {
      max-height: 200px;
      opacity: 1;
      margin-top: 12px;
    }

    body.dark .model-checkboxes {
      background: #1a1a1a;
    }

    .model-checkbox {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 10px;
      background: #fff;
      border: 1px solid #e8e8e8;
      border-radius: 8px;
      cursor: pointer;
      transition: all 0.2s ease;
      font-size: 12px;
    }

    body.dark .model-checkbox {
      background: #222;
      border-color: #333;
    }

    .model-checkbox:hover {
      border-color: #ccc;
    }

    body.dark .model-checkbox:hover {
      border-color: #444;
    }

    .model-checkbox.selected {
      border-color: #0a0a0a;
      background: #f0f0f0;
    }

    body.dark .model-checkbox.selected {
      border-color: #fafafa;
      background: #333;
    }

    .model-checkbox input[type="checkbox"] {
      width: 16px;
      height: 16px;
      accent-color: #0a0a0a;
    }

    body.dark .model-checkbox input[type="checkbox"] {
      accent-color: #fafafa;
    }

    .model-checkbox-label {
      flex: 1;
      color: #666;
      line-height: 1.3;
    }

    body.dark .model-checkbox-label {
      color: #999;
    }

    .model-checkbox.selected .model-checkbox-label {
      color: #0a0a0a;
      font-weight: 500;
    }

    body.dark .model-checkbox.selected .model-checkbox-label {
      color: #fafafa;
    }

    .model-count-hint {
      font-size: 11px;
      color: #888;
      margin-top: 8px;
      text-align: center;
    }

    /* Projects & History Sections */
    .content-section {
      flex: 1;
      padding: 32px;
      overflow-y: auto;
      display: none;
      background: #fff;
    }

    body.dark .content-section {
      background: #111;
    }

    .content-section.active {
      display: block;
    }

    .section-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 24px;
    }

    .section-title {
      font-size: 20px;
      font-weight: 600;
      color: #0a0a0a;
    }

    body.dark .section-title {
      color: #fafafa;
    }

    .btn-primary {
      padding: 10px 16px;
      border-radius: 10px;
      border: none;
      background: #0a0a0a;
      color: #fff;
      font-size: 13px;
      font-weight: 500;
      cursor: pointer;
      transition: all 0.2s ease;
    }

    body.dark .btn-primary {
      background: #fafafa;
      color: #0a0a0a;
    }

    .btn-primary:hover {
      opacity: 0.9;
      transform: translateY(-1px);
    }

    .btn-secondary {
      padding: 8px 14px;
      border-radius: 8px;
      border: 1px solid #e8e8e8;
      background: #fff;
      color: #0a0a0a;
      font-size: 12px;
      font-weight: 500;
      cursor: pointer;
      transition: all 0.2s ease;
    }

    body.dark .btn-secondary {
      background: #1a1a1a;
      border-color: #333;
      color: #fafafa;
    }

    .btn-secondary:hover {
      background: #f5f5f5;
    }

    body.dark .btn-secondary:hover {
      background: #222;
    }

    /* Cards */
    .card {
      background: #fafafa;
      border: 1px solid #e8e8e8;
      border-radius: 14px;
      padding: 20px;
      margin-bottom: 16px;
      transition: all 0.2s ease;
    }

    body.dark .card {
      background: #1a1a1a;
      border-color: #222;
    }

    .card:hover {
      border-color: #ccc;
    }

    body.dark .card:hover {
      border-color: #333;
    }

    /* Project Card */
    .project-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 16px;
    }

    .project-title {
      font-size: 15px;
      font-weight: 600;
      color: #0a0a0a;
    }

    body.dark .project-title {
      color: #fafafa;
    }

    .project-status {
      font-size: 11px;
      font-weight: 500;
      padding: 5px 12px;
      border-radius: 20px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }

    .status-active {
      background: #dcfce7;
      color: #166534;
    }

    body.dark .status-active {
      background: #14532d;
      color: #86efac;
    }

    .status-paused {
      background: #fef3c7;
      color: #92400e;
    }

    body.dark .status-paused {
      background: #78350f;
      color: #fde68a;
    }

    .status-completed {
      background: #e0e7ff;
      color: #3730a3;
    }

    body.dark .status-completed {
      background: #312e81;
      color: #a5b4fc;
    }

    /* Timeline */
    .project-timeline {
      border-left: 2px solid #e8e8e8;
      padding-left: 16px;
      margin-left: 4px;
    }

    body.dark .project-timeline {
      border-color: #333;
    }

    .timeline-item {
      position: relative;
      margin-bottom: 12px;
      font-size: 13px;
      color: #666;
      padding-left: 8px;
    }

    body.dark .timeline-item {
      color: #999;
    }

    .timeline-item::before {
      content: '';
      position: absolute;
      left: -21px;
      top: 6px;
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #e8e8e8;
    }

    body.dark .timeline-item::before {
      background: #333;
    }

    .timeline-item.high-risk::before {
      background: #ef4444;
    }

    .timeline-item.low-risk::before {
      background: #22c55e;
    }

    .timeline-item.positive::before {
      background: #3b82f6;
    }

    /* History */
    .session-group {
      margin-bottom: 28px;
    }

    .session-date {
      font-size: 13px;
      font-weight: 600;
      color: #666;
      margin-bottom: 12px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }

    body.dark .session-date {
      color: #888;
    }

    .history-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 16px;
      border-radius: 12px;
      background: #fafafa;
      border: 1px solid #e8e8e8;
      margin-bottom: 10px;
      cursor: pointer;
      transition: all 0.2s ease;
    }

    body.dark .history-item {
      background: #1a1a1a;
      border-color: #222;
    }

    .history-item:hover {
      background: #f0f0f0;
      border-color: #ccc;
    }

    body.dark .history-item:hover {
      background: #222;
      border-color: #333;
    }

    .history-title {
      font-size: 14px;
      font-weight: 500;
      color: #0a0a0a;
      margin-bottom: 4px;
    }

    body.dark .history-title {
      color: #fafafa;
    }

    .history-meta {
      font-size: 12px;
      color: #888;
    }

    body.dark .history-meta {
      color: #666;
    }

    .confidence-high {
      color: #22c55e;
    }

    .confidence-low {
      color: #ef4444;
    }

    /* Empty State */
    .empty-state {
      text-align: center;
      padding: 60px 20px;
      color: #888;
    }

    .empty-state-icon {
      font-size: 48px;
      margin-bottom: 16px;
      opacity: 0.5;
    }

    .empty-state-text {
      font-size: 14px;
    }

    /* Project Modal */
    .project-modal-overlay {
      position: fixed;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      background: rgba(0, 0, 0, 0.5);
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 1001;
      backdrop-filter: blur(4px);
    }

    .project-modal-overlay.active {
      display: flex;
    }

    .project-modal {
      background: #fff;
      border-radius: 16px;
      width: 100%;
      max-width: 480px;
      padding: 28px;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.2);
      animation: modalSlideIn 0.2s ease;
    }

    body.dark .project-modal {
      background: #1a1a1a;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
    }

    @keyframes modalSlideIn {
      from {
        transform: translateY(-20px);
        opacity: 0;
      }

      to {
        transform: translateY(0);
        opacity: 1;
      }
    }

    .project-modal-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 24px;
    }


    .project-modal-title {
      font-size: 18px;
      font-weight: 600;
      color: #0a0a0a;
    }

    /* News Section Styles */
    .news-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
      gap: 20px;
      margin-top: 20px;
    }

    .news-card {
      background: #fff;
      border: 1px solid #eee;
      border-radius: 12px;
      overflow: hidden;
      transition: transform 0.2s, box-shadow 0.2s;
      cursor: pointer;
      display: flex;
      flex-direction: column;
      height: 100%;
    }

    body.dark .news-card {
      background: #1a1a1a;
      border-color: #333;
    }

    .news-card:hover {
      transform: translateY(-2px);
      box-shadow: 0 8px 16px rgba(0, 0, 0, 0.05);
    }

    body.dark .news-card:hover {
      box-shadow: 0 8px 16px rgba(0, 0, 0, 0.3);
    }

    .news-image {
      height: 160px;
      width: 100%;
      background-color: #f5f5f5;
      background-size: cover;
      background-position: center;
      position: relative;
    }

    body.dark .news-image {
      background-color: #2a2a2a;
    }

    .news-badge {
      position: absolute;
      top: 12px;
      left: 12px;
      background: rgba(0, 0, 0, 0.7);
      color: white;
      padding: 4px 8px;
      border-radius: 4px;
      font-size: 11px;
      font-weight: 500;
    }

    .news-content {
      padding: 16px;
      flex: 1;
      display: flex;
      flex-direction: column;
    }

    .news-title {
      font-weight: 600;
      font-size: 15px;
      color: #111;
      margin-bottom: 8px;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
      line-height: 1.4;
    }

    body.dark .news-title {
      color: #eee;
    }

    .news-summary {
      font-size: 13px;
      color: #666;
      margin-bottom: 12px;
      display: -webkit-box;
      -webkit-line-clamp: 3;
      line-clamp: 3;
      -webkit-box-orient: vertical;
      overflow: hidden;
      line-height: 1.5;
    }

    body.dark .news-summary {
      color: #999;
    }

    .news-meta {
      margin-top: auto;
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 11px;
      color: #888;
      border-top: 1px solid #f0f0f0;
      padding-top: 12px;
    }

    body.dark .news-meta {
      border-top-color: #333;
      color: #666;
    }

    .news-source {
      font-weight: 500;
      color: #444;
    }

    body.dark .news-source {
      color: #aaa;
    }

    .refresh-news-btn {
      margin-left: auto;
      background: none;
      border: 1px solid #e0e0e0;
      padding: 6px 12px;
      border-radius: 6px;
      cursor: pointer;
      font-size: 12px;
      color: #666;
      transition: all 0.2s;
    }

    body.dark .refresh-news-btn {
      border-color: #444;
      color: #aaa;
    }

    .refresh-news-btn:hover {
      background: #f5f5f5;
      color: #333;
    }

    body.dark .refresh-news-btn:hover {
      background: #333;
      color: #fff;
    }

    body.dark .project-modal-title {
      color: #fafafa;
    }

    .project-modal-close {
      background: none;
      border: none;
      font-size: 20px;
      color: #666;
      cursor: pointer;
      padding: 4px 8px;
      border-radius: 6px;
      transition: all 0.2s ease;
    }

    .project-modal-close:hover {
      background: #f5f5f5;
      color: #0a0a0a;
    }

    body.dark .project-modal-close:hover {
      background: #333;
      color: #fafafa;
    }

    .project-form-group {
      margin-bottom: 20px;
    }

    .project-form-label {
      display: block;
      font-size: 13px;
      font-weight: 500;
      color: #666;
      margin-bottom: 8px;
    }

    body.dark .project-form-label {
      color: #999;
    }

    .project-form-input {
      width: 100%;
      padding: 12px 14px;
      border-radius: 10px;
      border: 1px solid #e8e8e8;
      font-size: 14px;
      font-family: inherit;
      background: #fafafa;
      color: #0a0a0a;
      transition: all 0.2s ease;
    }

    body.dark .project-form-input {
      background: #222;
      border-color: #333;
      color: #fafafa;
    }

    .project-form-input:focus {
      outline: none;
      border-color: #0a0a0a;
    }

    body.dark .project-form-input:focus {
      border-color: #fafafa;
    }

    .project-form-textarea {
      min-height: 100px;
      resize: vertical;
    }

    .project-status-selector {
      display: flex;
      gap: 10px;
    }

    .status-option {
      flex: 1;
      padding: 10px;
      border-radius: 8px;
      border: 1px solid #e8e8e8;
      background: #fafafa;
      text-align: center;
      cursor: pointer;
      font-size: 12px;
      font-weight: 500;
      transition: all 0.2s ease;
    }

    body.dark .status-option {
      background: #222;
      border-color: #333;
    }

    .status-option.active-status {
      border-color: #22c55e;
      background: #dcfce7;
      color: #166534;
    }

    body.dark .status-option.active-status {
      background: #14532d;
      color: #86efac;
    }

    .status-option.paused-status {
      border-color: #f59e0b;
      background: #fef3c7;
      color: #92400e;
    }

    body.dark .status-option.paused-status {
      background: #78350f;
      color: #fde68a;
    }

    .status-option:hover {
      border-color: #ccc;
    }

    .project-modal-actions {
      display: flex;
      gap: 12px;
      margin-top: 24px;
    }

    .btn-cancel {
      flex: 1;
      padding: 12px;
      border-radius: 10px;
      border: 1px solid #e8e8e8;
      background: #fff;
      color: #666;
      font-size: 14px;
      font-weight: 500;
      cursor: pointer;
      transition: all 0.2s ease;
    }

    body.dark .btn-cancel {
      background: #222;
      border-color: #333;
      color: #999;
    }

    .btn-cancel:hover {
      background: #f5f5f5;
    }

    body.dark .btn-cancel:hover {
      background: #333;
    }

    .btn-create {
      flex: 1;
      padding: 12px;
      border-radius: 10px;
      border: none;
      background: #0a0a0a;
      color: #fff;
      font-size: 14px;
      font-weight: 500;
      cursor: pointer;
      transition: all 0.2s ease;
    }

    body.dark .btn-create {
      background: #fafafa;
      color: #0a0a0a;
    }

    .btn-create:hover {
      opacity: 0.9;
      transform: translateY(-1px);
    }

    /* User Projects Container */
    #userProjectsContainer {
      margin-top: 16px;
    }

    .project-delete-btn {
      background: none;
      border: none;
      color: #ef4444;
      cursor: pointer;
      font-size: 12px;
      padding: 4px 8px;
      border-radius: 4px;
      margin-left: auto;
      transition: all 0.2s ease;
    }

    .project-delete-btn:hover {
      background: #fef2f2;
    }

    body.dark .project-delete-btn:hover {
      background: #450a0a;
    }
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
        const checkbox = document.querySelector(`#debate-${model} input`);
        const label = document.getElementById(`debate-${model}`);
        const isSelected = appSettings.debateModels.includes(model);
        checkbox.checked = isSelected;
        label.classList.toggle('selected', isSelected);
      });
    }

    function handleDebateModelChange(model) {
      const checkbox = document.querySelector(`#debate-${model} input`);
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
        const checkbox = document.querySelector(`#regret-${model} input`);
        const label = document.getElementById(`regret-${model}`);
        const isSelected = appSettings.regretModels.includes(model);
        checkbox.checked = isSelected;
        label.classList.toggle('selected', isSelected);
      });
    }

    function handleRegretModelChange(model) {
      const checkbox = document.querySelector(`#regret-${model} input`);
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
          const safeUrl = (article.url || '').replace(/'/g, "\\'");
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