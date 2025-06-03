// Check if auth is enabled via environment variable
const authEnabled = import.meta.env.VITE_APP_AUTH_ENABLED !== 'false';

export const jobmonAxiosConfig = {
    withCredentials: authEnabled, // Only send credentials when auth is enabled
    headers: {
        'Content-Type': 'application/json',
        Accept: 'application/json',
    },
};
