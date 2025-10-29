/**
 * ProtectedRoute Component
 * 
 * This component wraps pages that require authentication.
 * If the user is not logged in, they are redirected to the sign-in page.
 * 
 * Usage:
 * <Route path="/chat" element={
 *   <ProtectedRoute>
 *     <ChatPage />
 *   </ProtectedRoute>
 * } />
 */

import { useAuth } from '../../hooks/useAuth';
import { Navigate } from 'react-router-dom';

interface ProtectedRouteProps {
  children: React.ReactNode;
}

export default function ProtectedRoute({ children }: ProtectedRouteProps) {
  const { isAuthenticated, isLoading } = useAuth();

  // Show loading state while checking authentication
  if (isLoading) {
    return (
      <div style={{ 
        display: 'flex', 
        justifyContent: 'center', 
        alignItems: 'center', 
        minHeight: '100vh' 
      }}>
        <div style={{ fontSize: '1.2rem' }}>Loading...</div>
      </div>
    );
  }

  // If not authenticated, redirect to sign-in page
  // The 'replace' prop prevents users from going back to protected page
  if (!isAuthenticated) {
    return <Navigate to="/handler/sign-in" replace />;
  }

  // User is authenticated, show the protected content
  return <>{children}</>;
}

