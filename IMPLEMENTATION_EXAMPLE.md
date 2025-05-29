# Implementation Example: Optional Authentication

## Quick Start Implementation

This document shows the minimal changes needed to implement optional authentication.

### 1. Server Changes

#### Update `jobmon_server/src/jobmon/server/web/routes/utils.py`

```python
# Add these new functions after the existing code:

def get_user_or_anonymous(request: Request) -> User:
    """Get user or return anonymous user when auth is disabled."""
    config = JobmonConfig()
    try:
        auth_enabled = config.get_boolean("auth", "enabled")
    except ConfigError:
        auth_enabled = True  # Default to enabled for backwards compatibility
    
    if not auth_enabled:
        return create_anonymous_user()
    
    return get_user(request)

def create_anonymous_user() -> User:
    """Create an anonymous user for unauthenticated mode."""
    return User(
        sub="anonymous",
        email="anonymous@localhost", 
        preferred_username="anonymous",
        name="Anonymous User",
        updated_at=0,
        given_name="Anonymous",
        family_name="User",
        groups=["anonymous"],
        nonce="",
        at_hash="", 
        sid="",
        aud="",
        exp=0,
        iat=0,
        iss="localhost"
    )

def is_auth_enabled() -> bool:
    """Check if authentication is enabled."""
    config = JobmonConfig()
    try:
        return config.get_boolean("auth", "enabled")
    except ConfigError:
        return True  # Default to enabled
```

#### Update `jobmon_server/src/jobmon/server/web/api.py`

```python
# Modify the get_app function around line 83-89:

def get_app(versions: Optional[List[str]] = None) -> FastAPI:
    """Get a FastAPI app based on the config."""
    config = JobmonConfig()
    
    # Check if auth is enabled
    try:
        auth_enabled = config.get_boolean("auth", "enabled")
    except:
        auth_enabled = True  # Default to enabled
    
    # ... existing code until router inclusion ...
    
    # Include routers with conditional authentication
    versions = versions or (["auth", "v3", "v2"] if auth_enabled else ["v3", "v2"])
    url_prefix = "/api"
    
    for version in versions:
        mod = import_module(f"jobmon.server.web.routes.{version}")
        api_router = getattr(mod, f"api_{version}_router")
        
        dependencies = None
        if version == "v3":
            if auth_enabled:
                dependencies = [Depends(get_user)]
            else:
                # Import the new function
                from jobmon.server.web.routes.utils import get_user_or_anonymous
                dependencies = [Depends(get_user_or_anonymous)]
        
        app.include_router(api_router, prefix=url_prefix, dependencies=dependencies)
    
    # ... rest of the function ...
```

### 2. Frontend Changes

#### Create `jobmon_gui/.env.local` (for local development)

```env
# Set to false to disable authentication
VITE_APP_AUTH_ENABLED=false
```

#### Update `jobmon_gui/src/contexts/AuthContext.tsx`

```typescript
// Add this near the top of the AuthProvider component:

export const AuthProvider = (props: PropsWithChildren) => {
    // Check if auth is enabled via environment variable
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
                // Return anonymous user immediately
                localStorage.setItem('userInfo', JSON.stringify(anonymousUser));
                return anonymousUser;
            }
            
            // Original auth logic
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
            console.log('Authentication is disabled');
        }
    };

    const logoutHandler = () => {
        if (authEnabled) {
            localStorage.removeItem('userInfo');
            ExternalRedirect(logoutURL);
        } else {
            // No-op when auth is disabled  
            console.log('Authentication is disabled');
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
```

#### Update `jobmon_gui/src/components/navigation/PageNavigation.tsx`

```typescript
// Add this near the top of the component:

export default function PageNavigation({ children }: PropsWithChildren) {
    const theme = useTheme();
    const navigate = useNavigate();
    const [drawerOpen, setDrawerOpen] = useState(false);
    const { user } = useContext(AuthContext);
    const [userMenuAnchorEl, setUserMenuAnchorEl] = useState<null | HTMLElement>(null);
    
    // Check if auth is enabled
    const authEnabled = import.meta.env.VITE_APP_AUTH_ENABLED !== 'false';

    // ... existing code ...

    return (
        <Box sx={{ display: 'flex' }} id="AppBarBox">
            <CssBaseline />
            <AppBar position="fixed" sx={{ backgroundColor: '#17B9CF' }} id="AppBar">
                <Toolbar>
                    <Box sx={{ display: 'flex', flexGrow: 1, alignItems: 'center' }}>
                        {/* ... existing logo and title code ... */}
                    </Box>
                    
                    {/* Conditionally render user menu only if auth is enabled */}
                    {authEnabled && (
                        <Box>
                            <UserAvatarButton
                                userFullName={
                                    user.given_name || user.family_name
                                        ? `${user.given_name} ${user.family_name}`
                                        : ''
                                }
                                onClickHandler={setUserMenuAnchorEl}
                            />
                            <UserMenu
                                username={user.email || ''}
                                anchorEl={userMenuAnchorEl}
                                setAnchorEl={setUserMenuAnchorEl}
                            />
                        </Box>
                    )}
                    
                    {/* Optional: Show indicator when auth is disabled */}
                    {!authEnabled && (
                        <Box sx={{ color: 'white', fontSize: '0.8rem' }}>
                            Authentication Disabled
                        </Box>
                    )}
                </Toolbar>
            </AppBar>
            {/* ... rest of the component ... */}
        </Box>
    );
}
```

### 3. Configuration Files

#### Server Configuration Example

Create or update configuration file (e.g., `config.ini`):

```ini
[auth]
# Set to false to disable authentication completely
enabled = false

[oidc]
# These settings are ignored when auth.enabled = false
name = "your_oidc_provider"
conf_url = "https://your-provider/.well-known/openid_configuration"
client_id = "your_client_id"
client_secret = "your_client_secret"
scope = "openid email profile"
admin_group = "admin"
```

#### Docker Compose Example

```yaml
version: '3.8'
services:
  jobmon-server:
    # ... existing config ...
    environment:
      - JOBMON_AUTH__ENABLED=false
    
  jobmon-gui:
    # ... existing config ...
    environment:
      - VITE_APP_AUTH_ENABLED=false
```

### 4. Testing the Implementation

#### Test Auth Disabled Mode

1. **Server**: Set `[auth] enabled = false` in config
2. **Frontend**: Set `VITE_APP_AUTH_ENABLED=false` in environment
3. **Start both services**
4. **Verify**: 
   - No login screen appears
   - API calls work without authentication
   - User appears as "Anonymous User"
   - No user menu in navigation

#### Test Auth Enabled Mode (Default)

1. **Server**: Set `[auth] enabled = true` or remove the setting
2. **Frontend**: Set `VITE_APP_AUTH_ENABLED=true` or remove the setting  
3. **Start both services**
4. **Verify**:
   - Login screen appears for unauthenticated users
   - OIDC authentication flow works
   - User menu appears after login

### 5. Deployment Examples

#### Development (No Auth)
```bash
# Backend
export JOBMON_AUTH__ENABLED=false
python -m uvicorn jobmon.server.web.api:app

# Frontend  
echo "VITE_APP_AUTH_ENABLED=false" > .env.local
npm run dev
```

#### Production (With Auth)
```bash
# Backend - use production config with auth enabled
python -m uvicorn jobmon.server.web.api:app

# Frontend - build with auth enabled (default)
npm run build
```

This implementation provides a clean toggle between authenticated and unauthenticated modes while maintaining backward compatibility. 