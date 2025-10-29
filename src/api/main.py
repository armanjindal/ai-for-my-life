from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
from pydantic import BaseModel
from datetime import datetime
from typing import Literal
import logfire
import uvicorn

from fastapi import Request
from fastapi.responses import JSONResponse

import json
import asyncio
from typing import AsyncIterator

# We'll add imports from your existing code
from src.agents.finance_agent import finance_agent, FinanceAgentDeps
from src.services.db import (
    load_messages,
    save_messages,
    create_chat_session,
    get_user_sessions
)

# Configure Logfire for observability
logfire.configure()

# Lifespan context manager - runs on startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    This runs when the app starts and stops.
    We'll use it to set up resources that last the lifetime of the app.
    """
    # Startup: Create agent dependencies
    print("🚀 Starting FastAPI server...")
    
    # Store dependencies on app.state so all endpoints can access them
    app.state.finance_deps = FinanceAgentDeps(
        financial_goals=[
            "Spend less than $50/day on average",
            "Reduce dining out expenses",
            "Track all subscriptions"
        ]
    )
    
    yield  # Server is now running
    
    # Shutdown: Clean up resources (none needed here, but good to know about)
    print("👋 Shutting down...")

# Create FastAPI app
app = FastAPI(
    title="Finance Agent API",
    description="API for personal finance AI agent",
    version="0.1.0",
    lifespan=lifespan
)

# Add CORS middleware so your frontend can call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],  # Common frontend ports
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Instrument FastAPI with Logfire for tracing
logfire.instrument_fastapi(app)

# Health check endpoint
@app.get("/health")
async def health_check():
    """Simple endpoint to verify the API is running"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


# ============================================================================
# REQUEST/RESPONSE MODELS
# ============================================================================

class ChatRequest(BaseModel):
    """What the client sends when starting a chat"""
    prompt: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "prompt": "How much did I spend today?"
            }
        }

class MessageResponse(BaseModel):
    """
    Individual message in the conversation history.
    We simplify Pydantic AI's ModelMessage format for the frontend.
    """
    role: Literal["user", "model", "tool"]
    content: str
    timestamp: datetime

class SessionCreateRequest(BaseModel):
    """Request to create a new chat session"""
    title: str | None = None

class SessionResponse(BaseModel):
    """Session metadata returned to frontend"""
    session_id: str
    title: str
    preview_text: str | None
    message_count: int
    created_at: datetime
    updated_at: datetime

async def get_user_id() -> str:
    """
    Dependency that returns the current user ID.
    
    Later, you'll replace this with Neon Auth validation:
    - Extract JWT token from Authorization header
    - Validate token with Neon
    - Return the authenticated user_id
    
    For now, we hardcode it for development.
    """
    # TODO: Replace with real authentication
    # token = request.headers.get("Authorization")
    # user_id = validate_neon_token(token)
    # return user_id
    
    return "496a7de4-4e10-4a92-93be-248d176220cd"

async def get_finance_deps() -> FinanceAgentDeps:
    """
    Dependency that returns agent dependencies.
    Later, you could load user-specific financial goals from DB.
    """
    # TODO: Load user-specific goals from database
    # goals = get_user_financial_goals(user_id)
    
    return FinanceAgentDeps(
        financial_goals=[
            "Spend less than $50/day on average",
            "Reduce dining out expenses",
            "Track all subscriptions"
        ]
    )

# ============================================================================
# SESSION ENDPOINTS
# ============================================================================

@app.post("/api/sessions", response_model=dict)
async def create_session(
    request: SessionCreateRequest,
    user_id: str = Depends(get_user_id)
):
    """
    Create a new chat session.
    
    Flow:
    1. Client wants to start a new conversation
    2. We create a session in the database
    3. Return session_id to client
    4. Client uses this session_id for all subsequent messages
    """
    try:
        session_id = create_chat_session(
            user_id=user_id,
            title=request.title or "New Conversation"
        )
        
        return {
            "session_id": session_id,
            "message": "Session created successfully"
        }
    except Exception as e:
        logfire.error("Failed to create session", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sessions", response_model=list[SessionResponse])
async def list_sessions(
    limit: int = 20,
    offset: int = 0,
    user_id: str = Depends(get_user_id)
):
    """
    List all sessions for a user.
    
    This powers the "conversation history" sidebar in the UI.
    """
    try:
        sessions = get_user_sessions(user_id, limit=limit, offset=offset)
        
        # Convert to response model
        return [
            SessionResponse(
                session_id=s['session_id'],
                title=s['title'],
                preview_text=s.get('preview_text'),
                message_count=s['message_count'],
                created_at=s['created_at'],
                updated_at=s['updated_at']
            )
            for s in sessions
        ]
    except Exception as e:
        logfire.error("Failed to list sessions", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# CHAT HISTORY ENDPOINT
# ============================================================================

@app.get("/api/chat/{session_id}/history")
async def get_chat_history(
    session_id: str,
    user_id: str = Depends(get_user_id)
):
    """
    Get the full conversation history for a session.
    
    The frontend calls this when:
    - User opens an existing conversation
    - Page refreshes
    - User navigates back to a chat
    
    Returns messages in a simplified format for easy rendering.
    """
    try:
        # Load messages from database (returns Pydantic AI ModelMessage objects)
        messages = load_messages(session_id)
        
        # Convert to frontend-friendly format
        simplified_messages = []
        
        for msg in messages:
            # Extract the role from the message
            # Pydantic AI messages have 'kind' field: 'request' or 'response'
            if hasattr(msg, 'kind'):
                role = 'user' if msg.kind == 'request' else 'model'
            else:
                role = 'model'
            
            # Extract text content from message parts
            content = ""
            if hasattr(msg, 'parts'):
                for part in msg.parts:
                    if hasattr(part, 'content'):
                        content += str(part.content)
            
            simplified_messages.append({
                "role": role,
                "content": content,
                "timestamp": datetime.now().isoformat()  # You may want to store actual timestamps
            })
        
        return {
            "session_id": session_id,
            "messages": simplified_messages,
            "count": len(simplified_messages)
        }
        
    except Exception as e:
        logfire.error("Failed to load history", session_id=session_id, error=str(e))
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")




# ============================================================================
# STREAMING CHAT ENDPOINT
# ============================================================================

async def stream_agent_response(
    session_id: str,
    prompt: str,
    deps: FinanceAgentDeps
) -> AsyncIterator[str]:
    """
    Generator function that yields chunks of the agent's response.
    
    This is the heart of the streaming functionality.
    
    How it works:
    1. Load conversation history from DB
    2. Run agent with streaming enabled
    3. Yield each chunk of text as it's generated
    4. After streaming completes, save new messages to DB
    5. Send a final "done" message
    """
    
    try:
        # Step 1: Load conversation history
        conversation_history = load_messages(session_id)
        logfire.info("Loaded history", session_id=session_id, count=len(conversation_history))
        
        # Step 2: Run agent with streaming
        # The `async with` ensures proper cleanup
        async with finance_agent.run_stream(
            prompt,
            deps=deps,
            message_history=conversation_history
        ) as result:
            
            # Step 3: Stream the response chunk by chunk
            # Each chunk is a piece of text as the model generates it
            async for chunk in result.stream_text(delta=True):
                # Delta=True means we only get the new text, not the full text each time
                
                # Format as newline-delimited JSON (NDJSON)
                # Each line is a complete JSON object
                data = {
                    "type": "text",
                    "content": chunk,
                    "timestamp": datetime.now().isoformat()
                }
                
                # Yield with newline so frontend can split by line
                yield f"{json.dumps(data)}\n"
                
                # Small delay to avoid overwhelming the client
                await asyncio.sleep(0.01)
            
            # Step 4: Save the conversation to database
            # This happens AFTER streaming completes
            new_messages = result.new_messages()
            save_messages(session_id, new_messages)
            logfire.info("Saved messages", session_id=session_id, count=len(new_messages))
            
            # Step 5: Send completion signal
            completion_data = {
                "type": "complete",
                "content": "",
                "timestamp": datetime.now().isoformat()
            }
            yield f"{json.dumps(completion_data)}\n"
            
    except Exception as e:
        # If anything goes wrong, send an error message
        logfire.error("Streaming error", session_id=session_id, error=str(e))
        error_data = {
            "type": "error",
            "content": str(e),
            "timestamp": datetime.now().isoformat()
        }
        yield f"{json.dumps(error_data)}\n"


@app.post("/api/chat/{session_id}")
async def chat(
    session_id: str,
    request: ChatRequest,
    user_id: str = Depends(get_user_id),
    deps: FinanceAgentDeps = Depends(get_finance_deps)
):
    """
    Main chat endpoint - sends a message and streams the response.
    
    The frontend does:
    1. POST /api/chat/{session_id} with prompt
    2. Receives streaming response (newline-delimited JSON)
    3. Parses each line as JSON and renders it
    
    Response format:
    - Content-Type: text/plain (newline-delimited JSON)
    - Each line is a JSON object with type, content, timestamp
    - Types: "text" (chunk), "complete" (done), "error" (failed)
    """
    
    logfire.info(
        "Chat request",
        session_id=session_id,
        user_id=user_id,
        prompt=request.prompt[:100]  # Log first 100 chars
    )
    
    # Return a StreamingResponse
    # media_type="text/plain" tells the browser this is plain text, not HTML
    return StreamingResponse(
        stream_agent_response(session_id, request.prompt, deps),
        media_type="text/plain"
    )

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Catch-all error handler.
    Logs the error and returns a clean response to the client.
    """
    logfire.error(
        "Unhandled exception",
        path=request.url.path,
        method=request.method,
        error=str(exc)
    )
    
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "path": request.url.path
        }
    )

# ============================================================================
# DEVELOPMENT SERVER
# ============================================================================

if __name__ == "__main__":
    
    print("🚀 Starting Finance Agent API...")
    print("📖 Docs available at: http://localhost:8000/docs")
    print("🏥 Health check at: http://localhost:8000/health")
    
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Auto-reload on code changes
        log_level="info"
    )