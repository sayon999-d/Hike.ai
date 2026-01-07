#!/usr/bin/env python3
import re

with open('/Users/sayonmanna/project 2/backend/unified_ai.py', 'r') as f:
    content = f.read()

# Remove all HTML constant definitions
html_patterns = [
    r'LANDING_HTML = """[^"]*"""',
    r"NEWS_HTML = '''[^']*'''",
    r"REGRET_DASHBOARD_HTML = '''.*?'''",
    r"DEBATE_LOGIN_HTML = '''.*?'''",
    r"DEBATE_SIGNUP_HTML = '''.*?'''",
    r"DEBATE_HTML_CONTENT = '''.*?'''",
    r'SIMPLE_CSS = """.*?"""',
    r'def apply_theme\(html_content\):.*?return html_content'
]

for pattern in html_patterns:
    content = re.sub(pattern, '', content, flags=re.DOTALL)

# Clean up multiple blank lines
content = re.sub(r'\n\n\n+', '\n\n', content)

# Add template import at top
import_section = '''from fastapi import FastAPI, HTTPException, Request, Response, Cookie, WebSocket, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates'''

content = content.replace(
    'from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse',
    import_section
)

# Add templates initialization after app creation
app_creation = 'app = FastAPI(title="Unified AI System", description="News, Empathy, Debate, and Regret - All in one.")'
templates_init = '''
templates = Jinja2Templates(directory="templates")
'''
content = content.replace(app_creation, app_creation + templates_init)

# Write back
with open('/Users/sayonmanna/project 2/backend/unified_ai.py', 'w') as f:
    f.write(content)

print("✓ Removed all HTML constants")
print("✓ Added templates support")
