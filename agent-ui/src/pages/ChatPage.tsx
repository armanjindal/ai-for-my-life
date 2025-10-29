/**
 * ChatPage Component
 * 
 * Protected page for authenticated users to interact with the AI.
 * This is a placeholder that demonstrates the authentication pattern.
 * You'll expand this later to include actual chat functionality.
 */

import { useAuth } from '../hooks/useAuth';
import Header from '../components/layout/Header';
import ProtectedRoute from '../components/layout/ProtectedRoute';

function ChatPageContent() {
  const { user } = useAuth();

  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      <Header />
      
      <main style={{
        flex: 1,
        padding: '2rem',
        maxWidth: '1200px',
        margin: '0 auto',
        width: '100%',
      }}>
        <div style={{
          backgroundColor: 'rgba(100, 108, 255, 0.1)',
          border: '1px solid rgba(100, 108, 255, 0.3)',
          borderRadius: '8px',
          padding: '1.5rem',
          marginBottom: '2rem',
        }}>
          <h1>🤖 AI Chat Interface</h1>
          <p style={{ opacity: 0.8, marginTop: '0.5rem' }}>
            Welcome, <strong>{user?.displayName || user?.primaryEmail}</strong>!
          </p>
          <p style={{ opacity: 0.7, fontSize: '0.9rem', marginTop: '1rem' }}>
            This is a protected page. Only authenticated users can see this content.
          </p>
        </div>

        {/* Placeholder for Chat Interface */}
        <div style={{
          border: '1px solid rgba(255, 255, 255, 0.1)',
          borderRadius: '8px',
          padding: '2rem',
          textAlign: 'center',
          minHeight: '400px',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
        }}>
          <div style={{ fontSize: '4rem', marginBottom: '1rem' }}>💬</div>
          <h2>Chat Interface Coming Soon</h2>
          <p style={{ opacity: 0.7, marginTop: '1rem', maxWidth: '500px' }}>
            This is where your AI chat interface will go. You'll be able to:
          </p>
          <ul style={{ 
            textAlign: 'left', 
            marginTop: '1rem',
            opacity: 0.8,
            lineHeight: '1.8',
          }}>
            <li>Send messages to your AI assistant</li>
            <li>View streaming responses in real-time</li>
            <li>Access your chat history</li>
            <li>Create new conversation sessions</li>
          </ul>

          {/* Show User Info for Verification */}
          <div style={{
            marginTop: '2rem',
            padding: '1rem',
            backgroundColor: 'rgba(0, 0, 0, 0.2)',
            borderRadius: '8px',
            fontSize: '0.9rem',
          }}>
            <strong>Your User Info (for verification):</strong>
            <div style={{ marginTop: '0.5rem', fontFamily: 'monospace' }}>
              <div>ID: {user?.id}</div>
              <div>Email: {user?.primaryEmail}</div>
              <div>Name: {user?.displayName || 'Not set'}</div>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}

/**
 * Export the protected version of the page
 */
export default function ChatPage() {
  return (
    <ProtectedRoute>
      <ChatPageContent />
    </ProtectedRoute>
  );
}

