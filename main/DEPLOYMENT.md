# Deployment Guide

This application is containerized using Docker, making it easy to deploy on various platforms.

## Prerequisites

- A GitHub account (repo is located at https://github.com/sayon999-d/Hike.ai)
- API Keys for Google Gemini, Groq, OpenRouter, etc.

---

## Option 1: Deploy to Render.com (Recommended)

Render is the easiest way to deploy Docker or Python apps.

1.  **Create Account**: Sign up at [dashboard.render.com](https://dashboard.render.com).
2.  **New Web Service**: Click "New +" -> "Web Service".
3.  **Connect Repo**: Select your `Hike.ai` repository.
4.  **Configure**:
    *   **Name**: `hike-ai`
    *   **Runtime**: Docker
    *   **Region**: Singapore (or nearest to you)
5.  **Environment Variables**:
    Add the following keys (copy from your local `.env`):
    *   `GOOGLE_API_KEY`
    *   `GROQ_API_KEY`
    *   `OPENROUTER_API_KEY`
    *   `SECRET_KEY` (Generate a random string)
    *   `DATABASE_URL`: `sqlite:///data/unified_ai.db` (See Persistence note below)
6.  **Persistence (Important)**:
    Since we use SQLite, data is stored in a file. On Render free tier, files are lost on restart.
    *   *Upgrade to Paid*: Add a **Disk** to your service mounted at `/app/data`.
    *   *Alternative*: Switch to PostgreSQL (Render provides managed Postgres). Update `DATABASE_URL` to the Postgres connection string.
7.  **Deploy**: Click "Create Web Service".

---

## Option 2: Deploy to VPS (DigitalOcean / AWS EC2) using Docker

If you have a Linux server with Docker installed:

1.  **Clone the Repo**:
    ```bash
    git clone https://github.com/sayon999-d/Hike.ai.git
    cd Hike.ai/backend
    ```

2.  **Create .env File**:
    ```bash
    cp .env.example .env
    nano .env
    # Fill in your API keys
    ```

3.  **Build & Run**:
    ```bash
    docker build -t hike-ai .
    docker run -d -p 80:8000 --restart always --env-file .env -v $(pwd)/data:/app/data hike-ai
    ```
    *Note: We mount a volume `-v` to persist the SQLite database.*

---

## Option 3: GitHub Actions (CI/CD)

Your repository includes a CI/CD pipeline in `.github/workflows/ci_cd.yml`.

- **Current Behavior**:
    1.  On every push to `main`, it runs linting checks.
    2.  It builds the Docker image to ensure the code is deployable.

- **To Automate Deployment**:
    You can extend the workflow to push the image to Docker Hub or trigger a deployment hook on Render/Railway.

    *Example (Render Deploy Hook)*:
    Uncomment/Add this step to `ci_cd.yml`:
    ```yaml
    - name: Trigger Render Deploy
      run: curl -X POST ${{ secrets.RENDER_DEPLOY_HOOK }}
    ```
