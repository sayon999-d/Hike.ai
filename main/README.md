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

The system uses a modular, micro-orchestration architecture where Google Gemini acts as the central coordinator for specialized sub-systems.

### Components

1. Frontend: Responsive HTML/JS interface with real-time updates, project management, and settings
2. Orchestrator (Gemini 1.5): Analyzes user intent and routes requests to appropriate agents
3. Specialized Agents:
   - NewsFlow: Background thread fetching news every 60s with vector search
   - Debate Arena: Multiple AI personas debating from conflicting viewpoints
   - Regret AI: Predicts long-term emotional and practical regret of decisions
   - Empathetic Engine: Detects micro-emotions and tailors response tone

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

## API Documentation

Access interactive API docs at http://localhost:8000/docs

## License

MIT
