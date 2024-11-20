import {createContext, useState, useEffect, PropsWithChildren} from 'react'
import {Link} from "react-router-dom";
import {loginURL, logoutURL, userInfoURL} from "@jobmon_gui/configs/ApiUrls.ts";
import ExternalRedirect from "@jobmon_gui/utils/ExternalRedirect.ts"

export type UserType = {
    sub: string,
    email: string,
    preferred_username: string,
    name: string,
    updated_at: number,
    given_name: string,
    family_name: string,
    nonce: string,
    at_hash: string,
    sid: string,
    aud: string,
    exp: number,
    iat: number,
    iss: string,
    is_admin: boolean,
}

export type AuthContextType = {
    user: UserType | null
    checkUser: () => void
    loginHandler: () => void
    logoutHandler: () => void
}
const AuthContextDefaultValue: AuthContextType = {
    user: null,
    checkUser: () => {
    },
    loginHandler: () => {
    },
    logoutHandler: () => {
    },
}
const AuthContext = createContext<AuthContextType>(AuthContextDefaultValue)

export const AuthProvider = (props: PropsWithChildren) => {
    const [user, setUser] = useState<UserType | null>(null)


    // Check if user is logged in
    const checkUser = async () => {
        console.log("Running Check User")
        try {
            const res = await fetch(userInfoURL, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    // 'X-CSRFToken': cookieData,
                },
                credentials: "include",
            })
            const data = await res.json()
            if (res.ok && data.email) {
                setUser(data)
                console.log("Setting userInfo Localstorage")
                localStorage.setItem('userInfo', JSON.stringify(data))
            } else {
                setUser(null)
                console.log("Check user response not ok")
                localStorage.removeItem('userInfo')
                ExternalRedirect(loginURL)
            }
        } catch (e) {
            ExternalRedirect(loginURL)
        }
    }


    useEffect(() => {
        checkUser()
    }, [])

    const loginHandler = () => {
        ExternalRedirect(loginURL)
    }

    const logoutHandler = () => {
        setUser(null)
        localStorage.removeItem('userInfo')
        ExternalRedirect(logoutURL)
    }

    // Only render children if user is logged in
    if(user) {
        return (
            <AuthContext.Provider value={{user, checkUser, loginHandler, logoutHandler}}>
                {props.children}
            </AuthContext.Provider>
        )
    }
    // Render something in case the redirect fails
    return (
        <Link to={loginURL}></Link>
    )
}

export default AuthContext
