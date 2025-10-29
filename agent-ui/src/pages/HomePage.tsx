/**
 * HomePage Component
 * 
 * Main landing page that adapts based on authentication state:
 * - Not logged in: Marketing page encouraging sign-up
 * - Logged in: Personalized welcome page with quick actions
 */

import { useAuth } from '../hooks/useAuth';
import { Link } from 'react-router-dom';
import Header from '../components/layout/Header';

export default function HomePage() {
  const { user, isAuthenticated, isLoading } = useAuth();

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

  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      <Header />
      
      <main style={{
        flex: 1,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '2rem',
        textAlign: 'center',
      }}>
        {isAuthenticated ? (
          /* Authenticated User View */
          <AuthenticatedView user={user} />
        ) : (
          /* Public Landing Page View */
          <PublicView />
        )}
      </main>

      {/* Footer */}
      <footer style={{
        padding: '2rem',
        textAlign: 'center',
        opacity: 0.6,
        fontSize: '0.9rem',
        borderTop: '1px solid rgba(255, 255, 255, 0.1)',
      }}>
        <p>Built with React + TypeScript + Python FastAPI</p>
      </footer>
    </div>
  );
}

/**
 * View for authenticated users
 */
function AuthenticatedView({ user }: { user: any }) {
  return (
    <div>
      <h1>Welcome back, {user?.displayName || user?.primaryEmail?.split('@')[0]}! 👋</h1>
      <p style={{ 
        fontSize: '1.2rem', 
        opacity: 0.8, 
        marginBottom: '2rem',
        maxWidth: '600px',
      }}>
        Your AI-powered life assistant is ready to help you with finances,
        tasks, and insights.
      </p>
      
      {/* Action Buttons */}
      <div style={{ 
        display: 'flex', 
        gap: '1rem', 
        justifyContent: 'center',
        flexWrap: 'wrap',
        marginBottom: '3rem',
      }}>
        <Link to="/chat">
          <button style={{
            backgroundColor: '#646cff',
            color: 'white',
            fontSize: '1.1rem',
            padding: '0.8em 2em',
          }}>
            Start Chatting 💬
          </button>
        </Link>
      </div>

      {/* Feature Cards */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
        gap: '1.5rem',
        marginTop: '2rem',
        maxWidth: '900px',
      }}>
        <FeatureCard
          icon="💰"
          title="Finance Tracking"
          description="Get insights into your spending and financial goals"
        />
        <FeatureCard
          icon="📊"
          title="Smart Analytics"
          description="AI-powered analysis of your financial patterns"
        />
        <FeatureCard
          icon="🎯"
          title="Goal Setting"
          description="Set and track your financial objectives"
        />
      </div>
    </div>
  );
}

/**
 * View for non-authenticated visitors
 */
function PublicView() {
  return (
    <div>
      <h1>Welcome to Your AI Life Assistant 🚀</h1>
      <p style={{ 
        fontSize: '1.2rem', 
        opacity: 0.8, 
        marginBottom: '2rem',
        maxWidth: '600px',
        margin: '0 auto 2rem',
      }}>
        Manage your finances, track your goals, and get personalized
        insights powered by AI.
      </p>
      
      {/* Call to Action Buttons */}
      <div style={{ 
        display: 'flex', 
        gap: '1rem', 
        justifyContent: 'center',
        flexWrap: 'wrap',
        marginBottom: '3rem',
      }}>
        <Link to="/handler/sign-up">
          <button style={{
            backgroundColor: '#646cff',
            color: 'white',
            fontSize: '1.1rem',
            padding: '0.8em 2em',
          }}>
            Get Started Free 🎉
          </button>
        </Link>
        
        <Link to="/handler/sign-in">
          <button style={{
            fontSize: '1.1rem',
            padding: '0.8em 2em',
          }}>
            Sign In
          </button>
        </Link>
      </div>

      {/* Feature Showcase */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
        gap: '1.5rem',
        marginTop: '2rem',
        maxWidth: '900px',
      }}>
        <FeatureCard
          icon="🤖"
          title="AI-Powered Chat"
          description="Natural conversations about your finances"
        />
        <FeatureCard
          icon="🔒"
          title="Secure & Private"
          description="Your data is encrypted and protected"
        />
        <FeatureCard
          icon="⚡"
          title="Real-time Insights"
          description="Get instant answers to your questions"
        />
      </div>
    </div>
  );
}

/**
 * Reusable feature card component
 */
function FeatureCard({ 
  icon, 
  title, 
  description 
}: { 
  icon: string; 
  title: string; 
  description: string;
}) {
  return (
    <div style={{
      padding: '1.5rem',
      border: '1px solid rgba(255, 255, 255, 0.1)',
      borderRadius: '12px',
      backgroundColor: 'rgba(255, 255, 255, 0.02)',
      transition: 'transform 0.2s, border-color 0.2s',
    }}
    onMouseEnter={(e) => {
      e.currentTarget.style.transform = 'translateY(-4px)';
      e.currentTarget.style.borderColor = 'rgba(100, 108, 255, 0.5)';
    }}
    onMouseLeave={(e) => {
      e.currentTarget.style.transform = 'translateY(0)';
      e.currentTarget.style.borderColor = 'rgba(255, 255, 255, 0.1)';
    }}
    >
      <div style={{ fontSize: '2.5rem', marginBottom: '0.5rem' }}>{icon}</div>
      <h3 style={{ marginBottom: '0.5rem', fontSize: '1.2rem' }}>{title}</h3>
      <p style={{ opacity: 0.8, fontSize: '0.95rem', lineHeight: '1.5' }}>{description}</p>
    </div>
  );
}

