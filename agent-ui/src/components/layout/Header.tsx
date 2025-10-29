/**
 * Header Component
 * 
 * Navigation bar that shows different options based on authentication state:
 * - Not logged in: Shows "Sign In" and "Sign Up" buttons
 * - Logged in: Shows user email and "Sign Out" button
 */

import { useAuth } from '../../hooks/useAuth';
import { Link } from 'react-router-dom';

export default function Header() {
  const { user, isAuthenticated, signOut } = useAuth();

  return (
    <header style={{
      padding: '1rem 2rem',
      borderBottom: '1px solid rgba(255, 255, 255, 0.1)',
      display: 'flex',
      justifyContent: 'space-between',
      alignItems: 'center',
      backgroundColor: 'rgba(0, 0, 0, 0.2)',
    }}>
      {/* Logo / Home Link */}
      <Link to="/" style={{ 
        fontSize: '1.5rem', 
        fontWeight: 'bold',
        textDecoration: 'none',
        color: 'inherit',
      }}>
        🤖 AI Life Assistant
      </Link>

      {/* Navigation Links */}
      <nav style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
        {isAuthenticated ? (
          // Authenticated User Navigation
          <>
            <span style={{ fontSize: '0.9rem', opacity: 0.8 }}>
              {user?.primaryEmail || 'User'}
            </span>
            <Link to="/chat">
              <button>Chat</button>
            </Link>
            <button onClick={signOut}>Sign Out</button>
          </>
        ) : (
          // Non-authenticated User Navigation
          <>
            <Link to="/handler/sign-in">
              <button>Sign In</button>
            </Link>
            <Link to="/handler/sign-up">
              <button style={{ 
                backgroundColor: '#646cff',
                color: 'white',
              }}>
                Sign Up
              </button>
            </Link>
          </>
        )}
      </nav>
    </header>
  );
}

