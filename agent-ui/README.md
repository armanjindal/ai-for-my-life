# AI Life Assistant - Frontend

React + TypeScript frontend for the AI Life Assistant application.

## 🏗️ Architecture

```
src/
├── components/
│   └── layout/
│       ├── Header.tsx          # Navigation bar with auth state
│       └── ProtectedRoute.tsx  # Route guard component
├── hooks/
│   └── useAuth.ts             # Custom hook for authentication
├── pages/
│   ├── HomePage.tsx           # Landing/welcome page
│   └── ChatPage.tsx           # Protected chat interface
├── App.tsx                    # Main app with routing
├── stack.ts                   # Neon Auth (Stack) configuration
└── main.tsx                   # Entry point
```

## 🔐 Authentication Flow

This app uses **Neon Auth** (powered by Stack Auth) which provides:

1. **Frontend Authentication**: React hooks and components for auth UI
2. **Database Sync**: User data automatically synced to `neon_auth.users_sync` table in Neon Postgres

### How It Works

```
User Signs Up → Stack Auth Creates Account → Neon Syncs to Database → User Can Access Protected Pages
```

### Key Components

- **`useAuth()` hook**: Access current user and auth state
- **`<ProtectedRoute>`**: Wrap pages that require login
- **`/handler/*` routes**: Pre-built auth pages (sign-in, sign-up, etc.)

## 🚀 Getting Started

### 1. Install Dependencies

```bash
npm install
```

### 2. Set Up Environment Variables

Create a `.env` file:

```bash
VITE_STACK_PROJECT_ID=your_project_id
VITE_STACK_PUBLISHABLE_CLIENT_KEY=your_key
```

Get these from your Neon Console → Auth tab.

### 3. Run Development Server

```bash
npm run dev
```

Visit http://localhost:5173

## 📖 Usage Examples

### Using Authentication in Components

```tsx
import { useAuth } from '../hooks/useAuth';

function MyComponent() {
  const { user, isAuthenticated, isLoading } = useAuth();
  
  if (isLoading) return <div>Loading...</div>;
  if (!isAuthenticated) return <div>Please sign in</div>;
  
  return <div>Hello, {user.displayName}!</div>;
}
```

### Creating Protected Pages

```tsx
import ProtectedRoute from '../components/layout/ProtectedRoute';

function MyProtectedPage() {
  return (
    <ProtectedRoute>
      <div>Only authenticated users see this</div>
    </ProtectedRoute>
  );
}
```

## 🧪 Verification Steps

1. **Sign Up**: Visit `/handler/sign-up` and create an account
2. **Check Database**: User should appear in `neon_auth.users_sync` table
3. **Sign In**: Visit `/handler/sign-in` and log in
4. **Protected Route**: Visit `/chat` - should see chat page
5. **Sign Out**: Click "Sign Out" - should redirect to home page
6. **Access Control**: Try visiting `/chat` while logged out - should redirect to sign-in

## 🔗 Integration with Backend

When making API calls to your Python backend, include the auth token:

```tsx
const user = stackClientApp.getUser();
const authHeaders = await user?.getAuthHeaders();

fetch('http://localhost:8000/api/endpoint', {
  headers: {
    ...authHeaders,  // Includes Authorization: Bearer <token>
  }
});
```

Your Python backend can then validate this token and access user info from the `neon_auth.users_sync` table.

## 📚 Learn More

- [Neon Auth Docs](https://neon.tech/docs/guides/neon-auth)
- [Stack Auth React Docs](https://docs.stack-auth.com/getting-started/setup)
- [React Router Docs](https://reactrouter.com/)
- [TypeScript Handbook](https://www.typescriptlang.org/docs/)
