import { createContext, PropsWithChildren } from 'react';
import {
    loginURL,
    logoutURL,
    userInfoURL,
} from '@jobmon_gui/configs/ApiUrls.ts';
import ExternalRedirect from '@jobmon_gui/utils/ExternalRedirect.ts';
import LoginScreen from '@jobmon_gui/screens/Login.tsx';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';

export type UserType = {
    sub: string;
    email: string;
    preferred_username: string;
    name: string;
    updated_at: number;
    given_name: string;
    family_name: string;
    nonce: string;
    at_hash: string;
    sid: string;
    aud: string;
    exp: number;
    iat: number;
    iss: string;
    is_admin: boolean;
};

export type AuthContextType = {
    user: UserType | null;
    loginHandler: () => void;
    logoutHandler: () => void;
};
const AuthContextDefaultValue: AuthContextType = {
    user: null,
    loginHandler: () => {},
    logoutHandler: () => {},
};
const AuthContext = createContext<AuthContextType>(AuthContextDefaultValue);

export const AuthProvider = (props: PropsWithChildren) => {
    // Check if auth is enabled via environment variable
    // VITE_APP_AUTH_ENABLED defaults to true if not set
    const authEnabled = import.meta.env.VITE_APP_AUTH_ENABLED !== 'false';

    // Create anonymous user data for unauthenticated mode
    const anonymousUser: UserType = {
        sub: 'anonymous',
        email: 'anonymous@localhost',
        preferred_username: 'anonymous',
        name: 'Anonymous User',
        updated_at: 0,
        given_name: 'Anonymous',
        family_name: 'User',
        nonce: '',
        at_hash: '',
        sid: '',
        aud: '',
        exp: 0,
        iat: 0,
        iss: 'localhost',
        is_admin: false,
    };

    const user = useQuery({
        queryKey: ['userinfo'],
        queryFn: async () => {
            if (!authEnabled) {
                // Return anonymous user immediately when auth is disabled
                localStorage.setItem('userInfo', JSON.stringify(anonymousUser));
                return anonymousUser;
            }

            // Original auth logic for when auth is enabled
            return axios
                .get<UserType>(userInfoURL, jobmonAxiosConfig)
                .then(r => {
                    localStorage.setItem('userInfo', JSON.stringify(r.data));
                    return r.data;
                });
        },
        retry: false,
        enabled: true, // Always enabled, but returns anonymous user if auth disabled
    });

    const loginHandler = () => {
        if (authEnabled) {
            ExternalRedirect(loginURL);
        } else {
            // No-op when auth is disabled
            console.log('Authentication is disabled - login not available');
        }
    };

    const logoutHandler = () => {
        if (authEnabled) {
            localStorage.removeItem('userInfo');
            ExternalRedirect(logoutURL);
        } else {
            // No-op when auth is disabled
            console.log('Authentication is disabled - logout not available');
        }
    };

    // Only show login screen if auth is enabled AND there's an error
    if (authEnabled && (user.isError || !user.data)) {
        localStorage.removeItem('userInfo');
        return <LoginScreen />;
    }

    const userData = user.data || anonymousUser;

    return (
        <AuthContext.Provider
            value={{
                user: userData,
                loginHandler: loginHandler,
                logoutHandler: logoutHandler,
            }}
        >
            {props.children}
        </AuthContext.Provider>
    );
};

export default AuthContext;
