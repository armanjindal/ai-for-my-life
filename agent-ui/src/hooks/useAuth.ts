/**
 * Custom hook for accessing authentication state
 * 
 * This hook wraps Stack Auth's useUser() and provides a cleaner API
 * for checking authentication status throughout your app.
 * 
 * @returns {Object} Authentication state and user info
 * - user: The current user object (null if not logged in)
 * - isAuthenticated: Boolean indicating if user is logged in
 * - isLoading: Boolean indicating if auth state is still being determined
 * - userId: Quick access to user's ID
 * - userName: User's display name
 * - userEmail: User's email address
 * - signOut: Function to sign out the current user
 */

import { useUser } from '@stackframe/react';

export function useAuth() {
  // useUser returns:
  // - undefined: Still checking authentication (loading state)
  // - null: User is not logged in
  // - User object: User is logged in
  const user = useUser();
  
  return {
    user,
    isAuthenticated: !!user, // Convert to boolean: true if user exists
    isLoading: user === undefined, // Still determining auth state
    userId: user?.id,
    userName: user?.displayName,
    userEmail: user?.primaryEmail,
    signOut: async () => {
      await user?.signOut();
    },
  };
}

