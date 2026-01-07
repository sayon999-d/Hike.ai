# Hike.ai

Hike.ai is an AI platform that orchestrates multiple AI systems for decision support, empathetic conversation, real-time news analysis, and debate simulation.

## Features

- Empathetic Chat API with emotion detection and adaptive response strategies
- Debate Arena simulating multi-model AI debates using Groq, OpenRouter, Bytez, and Chutes
- Regret AI for decision analysis and future regret prediction
- News Flow for real-time news aggregation and semantic search
- Unified Orchestration via Google Gemini coordinating all subsystems
- Project Board for personal project management with timeline risk analysis

## System Architecture

```
                                    HIKE.AI SYSTEM ARCHITECTURE
                                    ===========================

    +-------------------+
    |   User Browser    |
    |   (HTML/CSS/JS)   |
    +--------+----------+
             |
             | HTTP/REST + WebSocket
             v
    +--------+----------+
    |    FastAPI        |
    |    Backend        |
    |                   |
    | - Auth Middleware |
    | - Rate Limiter    |
    | - Session Manager |
    +--------+----------+
             |
             v
    +--------+----------+
    |     GEMINI        |
    |   ORCHESTRATOR    |
    |                   |
    | Analyzes intent   |
    | Routes to agents  |
    | Synthesizes final |
    | response          |
    +--------+----------+
             |
             +------------------+------------------+------------------+
             |                  |                  |                  |
             v                  v                  v                  v
    +--------+------+  +-------+-------+  +-------+-------+  +-------+-------+
    |   NEWSFLOW    |  |  DEBATE AI    |  |  REGRET AI    |  | EMPATHETIC AI |
    |               |  |               |  |               |  |               |
    | - NewsAPI     |  | - Groq        |  | - Decision    |  | - Emotion     |
    | - Vector DB   |  | - OpenRouter  |  |   Analysis    |  |   Detection   |
    | - Embeddings  |  | - Chutes      |  | - Outcome     |  | - Strategy    |
    | - Summaries   |  | - Bytez       |  |   Prediction  |  |   Selection   |
    +-------+-------+  +-------+-------+  +-------+-------+  +-------+-------+
            |                  |                  |                  |
            +------------------+------------------+------------------+
                               |
                               v
                      +--------+----------+
                      |  UNIFIED RESPONSE |
                      |                   |
                      | Combined insights |
                      | from all agents   |
                      +-------------------+


    EXTERNAL SERVICES                     DATA LAYER
    =================                     ==========

    +-------------+                       +-------------+
    | NewsAPI.org |                       |   SQLite    |
    +-------------+                       | (Users, DB) |
                                          +-------------+
    +-------------+
    | Tavily API  |                       +-------------+
    | (Research)  |                       |    Redis    |
    +-------------+                       |  (Cache)    |
                                          +-------------+
    +-------------+
    | Google OAuth|
    +-------------+


    AI PROVIDERS (For Debate/Empathy)
    =================================

    +-------------+  +-------------+  +-------------+  +-------------+
    |    Groq     |  | OpenRouter  |  |   Chutes    |  |   Bytez     |
    | Llama 3.3   |  | DeepSeek R1 |  | Mistral 3.1 |  | Llama 3.1   |
    +-------------+  +-------------+  +-------------+  +-------------+
```

### Data Flow

1. User sends a message through the web interface
2. FastAPI receives the request and validates the session
3. Gemini Orchestrator analyzes the message intent
4. Based on intent, relevant agents are activated:
   - Research queries go to Tavily API
   - News context comes from NewsFlow
   - Decisions trigger Regret AI analysis
   - Debate questions spawn multi-model responses
   - All messages get empathetic processing
5. Orchestrator synthesizes all agent outputs into a unified response
6. Response is returned to the user with metadata

### Agent Responsibilities

| Agent | Purpose | Data Source |
|-------|---------|-------------|
| Orchestrator | Intent analysis, coordination, synthesis | Gemini 1.5 Flash |
| NewsFlow | Real-time news context | NewsAPI, Vector Search |
| Debate AI | Multi-perspective analysis | Groq, OpenRouter, Chutes, Bytez |
| Regret AI | Decision outcome prediction | Reasoning Models |
| Empathetic AI | Emotion detection, adaptive response | User-selected LLM |

## Tech Stack

- Backend: FastAPI, Python 3.10
- Database: SQLite (SQLAlchemy), Redis (Caching)
- AI Integration: Google Gemini, Groq, OpenRouter, Tavily, NewsAPI
- Frontend: HTML5, CSS3, Vanilla JS
- Containerization: Docker

## Installation

1. Clone the repository:
```bash
git clone https://github.com/sayon999-d/Hike.ai.git
cd Hike.ai/main
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure Environment:
```bash
cp .env.example .env
```
Fill in your API keys in the .env file.

## Running Locally

```bash
uvicorn unified_ai:app --reload
```

Access the application at http://localhost:8000

## Docker

Build and run with Docker:

```bash
docker build -t hike-ai .
docker run -p 8000:8000 --env-file .env hike-ai
```

## Deployment

For deployment instructions (Render, VPS, CI/CD), see DEPLOYMENT.md

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | / | Main application UI |
| POST | /api/login | User authentication |
| POST | /api/signup | User registration |
| GET | /api/profile | Get user profile |
| POST | /api/chat | Send message to AI |
| GET | /api/news/latest | Get latest news |
| POST | /api/debate | Start multi-model debate |
| POST | /api/decide | Get decision analysis |
| WS | /ws/chat | WebSocket for real-time chat |

## API Documentation

Access interactive API docs at http://localhost:8000/docs

## License

MIT
