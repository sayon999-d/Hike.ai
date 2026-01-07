# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

We take security seriously. If you discover a security vulnerability within this project, please follow these steps:

1.  **Do NOT create a public GitHub issue.** Rapid disclosure can put users at risk.
2.  Email our security team at `security@hike.ai` (or the repository owner).
3.  Include a detailed description of the vulnerability, steps to reproduce, and potential impact.

We will acknowledge your report within 48 hours and provide a timeline for a fix.

## Implementation Details

This application implements several security best practices to ensure user safety and data integrity:

### 1. Rate Limiting
- **Token Bucket Algorithm**: Controls outgoing requests to third-party AI providers to prevent quota exhaustion.
- **User Rate Limiting**: Limit of 60 requests per minute per IP address on API endpoints to prevent DDoS and abuse.
- **Auth Limiting**: Strict limits on Login/Signup endpoints.

### 2. HTTP Security Headers
The application enforces strict security headers via `SecurityMiddleware`:
- `Content-Security-Policy`: Restricts resource loading sources.
- `X-Frame-Options: DENY`: Prevents clickjacking.
- `X-Content-Type-Options: nosniff`: Prevents MIME-type sniffing.
- `Strict-Transport-Security`: Enforces HTTPS (HSTS).

### 3. Input Sanitization
- All inputs are validated via Pydantic models.
- Output encoding prevents XSS in the frontend.
- Markdown rendering is sanitized.

### 4. Authentication
- Secure session management with HTTPOnly and Secure cookies.
- OAuth integration (Google) for secure identity verification.

## Third-Party AI Models

Please note that user data (prompts) is sent to third-party AI providers (Google Gemini, Groq, OpenRouter, etc.) for processing. We ensure no personally identifiable information (PII) is explicitly logged, but users should be advised not to share sensitive secrets in chat.
