# FastAPI Endpoints for Chat Application

This document describes the REST API endpoints for the AI chat application.

## Base URL
```
Development: http://localhost:8000
Production: https://api.yourapp.com
```

## Authentication
All endpoints (except `/health`) require authentication.

```http
Authorization: Bearer <jwt_token>
```

User ID is extracted from JWT token for authorization.

---

## Endpoints Overview

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check |
| `GET` | `/api/sessions` | List user's chat sessions |
| `POST` | `/api/sessions` | Create new chat session |
| `GET` | `/api/sessions/{session_id}` | Get session details |
| `PATCH` | `/api/sessions/{session_id}` | Update session (title, archive) |
| `DELETE` | `/api/sessions/{session_id}` | Delete session |
| `GET` | `/api/sessions/{session_id}/messages` | Get messages in a session |
| `POST` | `/api/sessions/{session_id}/messages` | Send message (streaming) |
| `DELETE` | `/api/messages/{message_id}` | Delete a message |
| `GET` | `/api/search` | Search across all sessions |
| `GET` | `/api/usage` | Get usage statistics |

---

## Endpoint Details

### 1. Health Check

```http
GET /health
```

**Description:** Check if API is running

**Authentication:** None

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-10-27T10:30:00Z",
  "version": "1.0.0"
}
```

---

### 2. List Chat Sessions

```http
GET /api/sessions
```

**Description:** Get list of user's chat sessions

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int | 20 | Max sessions to return |
| `offset` | int | 0 | Pagination offset |
| `archived` | bool | false | Include archived sessions |
| `sort` | string | "updated_at" | Sort field (updated_at, created_at, title) |
| `order` | string | "desc" | Sort order (asc, desc) |

**Example Request:**
```bash
GET /api/sessions?limit=10&archived=false
Authorization: Bearer <token>
```

**Response:**
```json
{
  "sessions": [
    {
      "session_id": "550e8400-e29b-41d4-a716-446655440000",
      "title": "October Spending Analysis",
      "preview_text": "You spent $45 today on groceries and dining...",
      "model": "anthropic:claude-3-5-haiku-20241022",
      "message_count": 15,
      "created_at": "2025-10-27T08:00:00Z",
      "updated_at": "2025-10-27T10:30:00Z",
      "archived": false
    },
    {
      "session_id": "660e8400-e29b-41d4-a716-446655440001",
      "title": "September Budget Review",
      "preview_text": "Your total spending for September was $1,234...",
      "model": "anthropic:claude-3-5-haiku-20241022",
      "message_count": 8,
      "created_at": "2025-10-01T14:20:00Z",
      "updated_at": "2025-10-01T15:45:00Z",
      "archived": false
    }
  ],
  "total": 25,
  "limit": 10,
  "offset": 0
}
```

**Status Codes:**
- `200 OK`: Success
- `401 Unauthorized`: Invalid/missing token

---

### 3. Create Chat Session

```http
POST /api/sessions
```

**Description:** Create a new chat session

**Request Body:**
```json
{
  "title": "New Budget Chat",  // Optional, auto-generated if not provided
  "metadata": {                // Optional
    "financial_goals": [
      "Spend less than $50/day",
      "Track subscriptions"
    ]
  }
}
```

**Response:**
```json
{
  "session_id": "770e8400-e29b-41d4-a716-446655440002",
  "title": "New Budget Chat",
  "model": "anthropic:claude-3-5-haiku-20241022",
  "created_at": "2025-10-27T10:35:00Z",
  "updated_at": "2025-10-27T10:35:00Z",
  "archived": false,
  "metadata": {
    "financial_goals": [
      "Spend less than $50/day",
      "Track subscriptions"
    ]
  }
}
```

**Status Codes:**
- `201 Created`: Session created
- `400 Bad Request`: Invalid request body
- `401 Unauthorized`: Invalid/missing token

---

### 4. Get Session Details

```http
GET /api/sessions/{session_id}
```

**Description:** Get detailed information about a session

**Path Parameters:**
- `session_id` (UUID): Session identifier

**Response:**
```json
{
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "title": "October Spending Analysis",
  "preview_text": "You spent $45 today...",
  "model": "anthropic:claude-3-5-haiku-20241022",
  "message_count": 15,
  "total_tokens": 3450,
  "estimated_cost": 0.15,
  "created_at": "2025-10-27T08:00:00Z",
  "updated_at": "2025-10-27T10:30:00Z",
  "archived": false,
  "metadata": {
    "financial_goals": [...]
  }
}
```

**Status Codes:**
- `200 OK`: Success
- `404 Not Found`: Session doesn't exist or not owned by user

---

### 5. Update Session

```http
PATCH /api/sessions/{session_id}
```

**Description:** Update session properties

**Request Body:**
```json
{
  "title": "Updated Title",     // Optional
  "archived": true,              // Optional
  "metadata": {...}              // Optional
}
```

**Response:**
```json
{
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "title": "Updated Title",
  "archived": true,
  "updated_at": "2025-10-27T10:40:00Z"
}
```

**Status Codes:**
- `200 OK`: Updated successfully
- `404 Not Found`: Session not found
- `400 Bad Request`: Invalid request body

---

### 6. Delete Session

```http
DELETE /api/sessions/{session_id}
```

**Description:** Permanently delete a session and all its messages

**Response:**
```json
{
  "message": "Session deleted successfully",
  "session_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Status Codes:**
- `200 OK`: Deleted successfully
- `404 Not Found`: Session not found

**Note:** Consider soft-deletion (archive) instead for data recovery.

---

### 7. Get Session Messages

```http
GET /api/sessions/{session_id}/messages
```

**Description:** Get all messages in a conversation

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int | 50 | Max messages to return |
| `before` | UUID | null | Get messages before this ID (pagination) |
| `after` | UUID | null | Get messages after this ID (pagination) |

**Example Request:**
```bash
GET /api/sessions/550e8400.../messages?limit=50
Authorization: Bearer <token>
```

**Response:**
```json
{
  "messages": [
    {
      "id": "msg-001",
      "role": "user",
      "content": "How much did I spend today?",
      "created_at": "2025-10-27T10:00:00Z",
      "tokens_used": 8
    },
    {
      "id": "msg-002",
      "role": "model",
      "content": "You spent $45 today on the following transactions:\n\n• $20 at Starbucks\n• $25 at Whole Foods\n\nThis is within your $50/day goal!",
      "created_at": "2025-10-27T10:00:15Z",
      "tokens_used": 120,
      "tool_calls": [
        {
          "tool_name": "get_transactions",
          "arguments": {"days": 1}
        }
      ]
    },
    {
      "id": "msg-003",
      "role": "user",
      "content": "What about this week?",
      "created_at": "2025-10-27T10:02:00Z",
      "tokens_used": 6
    }
  ],
  "total": 3,
  "has_more": false
}
```

**Status Codes:**
- `200 OK`: Success
- `404 Not Found`: Session not found

---

### 8. Send Message (Streaming)

```http
POST /api/sessions/{session_id}/messages
```

**Description:** Send a message and get streaming response from AI agent

**Request Body:**
```json
{
  "message": "How much did I spend today?"
}
```

**Response:** Server-Sent Events (SSE) stream

**Content-Type:** `text/event-stream`

**Event Stream Format:**
```
event: message_start
data: {"message_id": "msg-uuid", "role": "model"}

event: content_delta
data: {"text": "You"}

event: content_delta
data: {"text": " spent"}

event: content_delta
data: {"text": " $45"}

event: tool_call
data: {"tool_name": "get_transactions", "arguments": {"days": 1}}

event: content_delta
data: {"text": " today on the following transactions..."}

event: message_end
data: {"message_id": "msg-uuid", "tokens_used": 120, "created_at": "2025-10-27T10:30:00Z"}
```

**Usage Example (JavaScript):**
```javascript
const eventSource = new EventSource('/api/sessions/session-id/messages', {
  method: 'POST',
  headers: {
    'Authorization': `Bearer ${token}`,
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({ message: 'How much did I spend?' })
});

eventSource.addEventListener('content_delta', (e) => {
  const data = JSON.parse(e.data);
  appendText(data.text);
});

eventSource.addEventListener('message_end', (e) => {
  const data = JSON.parse(e.data);
  console.log(`Used ${data.tokens_used} tokens`);
  eventSource.close();
});
```

**Status Codes:**
- `200 OK`: Stream started
- `404 Not Found`: Session not found
- `400 Bad Request`: Invalid message

---

### 9. Delete Message

```http
DELETE /api/messages/{message_id}
```

**Description:** Soft-delete a message (mark as deleted)

**Response:**
```json
{
  "message": "Message deleted successfully",
  "message_id": "msg-uuid"
}
```

**Status Codes:**
- `200 OK`: Deleted
- `404 Not Found`: Message not found
- `403 Forbidden`: Not authorized to delete this message

---

### 10. Search Messages

```http
GET /api/search
```

**Description:** Full-text search across all user's messages

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `q` | string | Yes | Search query |
| `limit` | int | No | Max results (default: 20) |
| `session_id` | UUID | No | Limit to specific session |

**Example Request:**
```bash
GET /api/search?q=spending&limit=10
Authorization: Bearer <token>
```

**Response:**
```json
{
  "results": [
    {
      "message_id": "msg-001",
      "session_id": "session-001",
      "session_title": "October Budget",
      "role": "model",
      "content": "Your spending this month is $1,234...",
      "created_at": "2025-10-15T10:00:00Z",
      "relevance_score": 0.95
    },
    {
      "message_id": "msg-045",
      "session_id": "session-002",
      "session_title": "Daily Check-in",
      "role": "user",
      "content": "How's my spending compared to last week?",
      "created_at": "2025-10-20T14:30:00Z",
      "relevance_score": 0.87
    }
  ],
  "total": 24,
  "query": "spending"
}
```

**Status Codes:**
- `200 OK`: Success
- `400 Bad Request`: Missing or invalid query

---

### 11. Get Usage Statistics

```http
GET /api/usage
```

**Description:** Get usage metrics and costs

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `period` | string | "30d" | Time period (7d, 30d, 90d, all) |
| `session_id` | UUID | null | Limit to specific session |

**Response:**
```json
{
  "period": "30d",
  "statistics": {
    "total_sessions": 12,
    "total_messages": 156,
    "total_tokens": 45320,
    "estimated_cost": 2.15,
    "breakdown": {
      "input_tokens": 12450,
      "output_tokens": 32870
    }
  },
  "by_model": {
    "anthropic:claude-3-5-haiku-20241022": {
      "messages": 156,
      "tokens": 45320,
      "cost": 2.15
    }
  },
  "daily_usage": [
    {
      "date": "2025-10-27",
      "messages": 8,
      "tokens": 2340,
      "cost": 0.11
    },
    {
      "date": "2025-10-26",
      "messages": 12,
      "tokens": 3890,
      "cost": 0.18
    }
  ]
}
```

**Status Codes:**
- `200 OK`: Success

---

## Error Responses

All errors follow this format:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid session_id format",
    "details": {
      "field": "session_id",
      "issue": "Must be a valid UUID"
    }
  },
  "timestamp": "2025-10-27T10:30:00Z"
}
```

### Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `VALIDATION_ERROR` | 400 | Request validation failed |
| `UNAUTHORIZED` | 401 | Missing or invalid auth token |
| `FORBIDDEN` | 403 | Not authorized for this resource |
| `NOT_FOUND` | 404 | Resource doesn't exist |
| `RATE_LIMIT_EXCEEDED` | 429 | Too many requests |
| `INTERNAL_ERROR` | 500 | Server error |
| `MODEL_ERROR` | 503 | AI model unavailable |

---

## Rate Limiting

```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1698412800
```

**Limits:**
- **Standard**: 100 requests per minute per user
- **Message sending**: 20 requests per minute per user
- **Search**: 30 requests per minute per user

**Response when exceeded:**
```json
{
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "Too many requests. Try again in 45 seconds.",
    "retry_after": 45
  }
}
```

---

## Pagination

### Cursor-Based Pagination (Recommended for messages)

```bash
GET /api/sessions/{id}/messages?limit=50&after=msg-uuid
```

Response includes cursor for next page:
```json
{
  "messages": [...],
  "pagination": {
    "next_cursor": "msg-uuid-50",
    "has_more": true
  }
}
```

### Offset-Based Pagination (For session lists)

```bash
GET /api/sessions?limit=20&offset=40
```

Response includes total count:
```json
{
  "sessions": [...],
  "total": 125,
  "limit": 20,
  "offset": 40
}
```

---

## WebSocket Alternative (Optional)

For bi-directional real-time communication:

```javascript
const ws = new WebSocket('ws://localhost:8000/ws/chat');

ws.onopen = () => {
  ws.send(JSON.stringify({
    type: 'auth',
    token: 'jwt-token'
  }));
  
  ws.send(JSON.stringify({
    type: 'message',
    session_id: 'session-uuid',
    content: 'How much did I spend?'
  }));
};

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  
  switch(data.type) {
    case 'content_delta':
      appendText(data.text);
      break;
    case 'message_complete':
      console.log('Response complete');
      break;
  }
};
```

---

## Frontend Integration Example

### React Hook for Streaming Messages

```typescript
function useChatStream(sessionId: string) {
  const [messages, setMessages] = useState([]);
  const [isStreaming, setIsStreaming] = useState(false);
  
  const sendMessage = async (content: string) => {
    setIsStreaming(true);
    
    // Add user message immediately
    const userMessage = {
      role: 'user',
      content,
      created_at: new Date().toISOString()
    };
    setMessages(prev => [...prev, userMessage]);
    
    // Stream agent response
    const response = await fetch(`/api/sessions/${sessionId}/messages`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ message: content })
    });
    
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    
    let agentMessage = {
      role: 'model',
      content: '',
      created_at: new Date().toISOString()
    };
    
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      
      const chunk = decoder.decode(value);
      const lines = chunk.split('\n');
      
      for (const line of lines) {
        if (line.startsWith('data: ')) {
          const data = JSON.parse(line.slice(6));
          
          if (data.text) {
            agentMessage.content += data.text;
            setMessages(prev => {
              const updated = [...prev];
              updated[updated.length - 1] = {...agentMessage};
              return updated;
            });
          }
        }
      }
    }
    
    setIsStreaming(false);
  };
  
  return { messages, sendMessage, isStreaming };
}
```

---

## Testing

### Example cURL Commands

**Create session:**
```bash
curl -X POST http://localhost:8000/api/sessions \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"title": "Test Chat"}'
```

**List sessions:**
```bash
curl http://localhost:8000/api/sessions \
  -H "Authorization: Bearer $TOKEN"
```

**Send message:**
```bash
curl -X POST http://localhost:8000/api/sessions/$SESSION_ID/messages \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"message": "How much did I spend today?"}' \
  --no-buffer
```

**Search:**
```bash
curl "http://localhost:8000/api/search?q=spending&limit=10" \
  -H "Authorization: Bearer $TOKEN"
```

