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
    // const [user, setUser] = useState<UserType | null>(null)

    const user = useQuery({
        queryKey: ['userinfo'],
        queryFn: async () => {
            return axios
                .get<UserType>(userInfoURL, jobmonAxiosConfig)
                .then(r => {
                    localStorage.setItem('userInfo', JSON.stringify(r.data));
                    return r.data;
                });
        },
        retry: false,
    });

    const loginHandler = () => {
        ExternalRedirect(loginURL);
    };

    const logoutHandler = () => {
        localStorage.removeItem('userInfo');
        ExternalRedirect(logoutURL);
    };
    if (user.isError || !user.data) {
        localStorage.removeItem('userInfo');
        return <LoginScreen />;
    }
    const userData = user.data;
    // Render something in case the redirect fails
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
