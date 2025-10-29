/**
 * App Component - Main Application Entry Point
 * 
 * This component sets up the routing and authentication structure for the app.
 * 
 * Key Components:
 * - BrowserRouter: Enables client-side routing
 * - StackProvider: Makes authentication state available to all components
 * - StackTheme: Provides pre-styled auth UI components
 * 
 * Routes:
 * - /: Home page (adapts based on auth state)
 * - /chat: Protected chat interface (requires login)
 * - /handler/*: Authentication pages (sign-in, sign-up, etc.)
 */

import { StackHandler, StackProvider, StackTheme } from '@stackframe/react';
import { Suspense } from 'react';
import { BrowserRouter, Route, Routes, useLocation } from 'react-router-dom';
import { stackClientApp } from './stack';

// Import our pages
import HomePage from './pages/HomePage';
import ChatPage from './pages/ChatPage';

/**
 * HandlerRoutes Component
 * 
 * This handles all authentication-related routes like:
 * - /handler/sign-in
 * - /handler/sign-up
 * - /handler/forgot-password
 * - etc.
 * 
 * Stack Auth provides pre-built UI for these pages.
 */
function HandlerRoutes() {
  const location = useLocation();
  return (
    <StackHandler app={stackClientApp} location={location.pathname} fullPage />
  );
}

/**
 * Main App Component
 */
export default function App() {
  return (
    <Suspense fallback={
      <div style={{ 
        display: 'flex', 
        justifyContent: 'center', 
        alignItems: 'center', 
        minHeight: '100vh' 
      }}>
        Loading...
      </div>
    }>
      <BrowserRouter>
        {/* StackProvider makes authentication available throughout the app */}
        <StackProvider app={stackClientApp}>
          {/* StackTheme provides styling for auth components */}
          <StackTheme>
            <Routes>
              {/* Authentication handler routes */}
              <Route path="/handler/*" element={<HandlerRoutes />} />
              
              {/* Main application routes */}
              <Route path="/" element={<HomePage />} />
              <Route path="/chat" element={<ChatPage />} />
              
              {/* You can add more routes here as you build features */}
            </Routes>
          </StackTheme>
        </StackProvider>
      </BrowserRouter>
    </Suspense>
  );
}