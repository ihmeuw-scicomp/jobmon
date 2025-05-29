# Design: Optional Authentication for Jobmon GUI ↔ Server

## Overview
This design enables optional authentication between the jobmon GUI and server, allowing the system to operate in both authenticated and unauthenticated modes based on configuration.

## Core Changes Required

### 1. Server-Side Changes

#### A. Configuration Enhancement
Add a new configuration option to control authentication mode:

```ini
[auth]
enabled = true  # Set to false to disable authentication
```

#### B. Authentication Middleware/Dependency Modification
Create a conditional authentication dependency that can optionally bypass user validation:

**File**: `jobmon_server/src/jobmon/server/web/routes/utils.py`
```python
def get_user_optional(request: Request) -> Optional[User]:
    """Optional user retrieval that returns None when auth is disabled."""
    config = JobmonConfig()
    auth_enabled = config.get_boolean("auth", "enabled", default=True)
    
    if not auth_enabled:
        return None
    
    return get_user(request)

def get_user_or_anonymous(request: Request) -> User:
    """Get user or return anonymous user when auth is disabled."""
    config = JobmonConfig()
    auth_enabled = config.get_boolean("auth", "enabled", default=True)
    
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
```

#### C. API Route Protection Update
**File**: `jobmon_server/src/jobmon/server/web/api.py`
```python
def get_app(versions: Optional[List[str]] = None) -> FastAPI:
    """Get a FastAPI app based on the config."""
    config = JobmonConfig()
    auth_enabled = config.get_boolean("auth", "enabled", default=True)
    
    # ... existing code ...
    
    # Include routers with conditional authentication
    versions = versions or ["auth", "v3", "v2"] if auth_enabled else ["v3", "v2"]
    url_prefix = "/api"
    
    for version in versions:
        mod = import_module(f"jobmon.server.web.routes.{version}")
        api_router = getattr(mod, f"api_{version}_router")
        
        dependencies = None
        if version == "v3":
            if auth_enabled:
                dependencies = [Depends(get_user)]
            else:
                dependencies = [Depends(get_user_or_anonymous)]
        
        app.include_router(api_router, prefix=url_prefix, dependencies=dependencies)
```

#### D. Auth Route Conditional Loading
**File**: `jobmon_server/src/jobmon/server/web/auth.py`
```python
def setup_oauth_conditionally():
    """Setup OAuth only if authentication is enabled."""
    config = JobmonConfig()
    auth_enabled = config.get_boolean("auth", "enabled", default=True)
    
    if not auth_enabled:
        return None
        
    # ... existing OAuth setup code ...
    return oauth
```

### 2. Frontend Changes

#### A. Environment Variable for Auth Mode
**File**: `jobmon_gui/.env` (or environment-specific files)
```
VITE_APP_AUTH_ENABLED=true  # Set to false to disable authentication
```

#### B. Conditional Authentication Context
**File**: `jobmon_gui/src/contexts/AuthContext.tsx`
```typescript
export const AuthProvider = (props: PropsWithChildren) => {
    const authEnabled = import.meta.env.VITE_APP_AUTH_ENABLED !== 'false';
    
    const user = useQuery({
        queryKey: ['userinfo'],
        queryFn: async () => {
            if (!authEnabled) {
                // Return mock user for unauthenticated mode
                return {
                    sub: 'anonymous',
                    email: 'anonymous@localhost',
                    preferred_username: 'anonymous',
                    name: 'Anonymous User',
                    // ... other required fields
                };
            }
            
            return axios
                .get<UserType>(userInfoURL, jobmonAxiosConfig)
                .then(r => {
                    localStorage.setItem('userInfo', JSON.stringify(r.data));
                    return r.data;
                });
        },
        retry: false,
        enabled: authEnabled, // Only run query if auth is enabled
    });

    const loginHandler = () => {
        if (authEnabled) {
            ExternalRedirect(loginURL);
        }
    };

    const logoutHandler = () => {
        if (authEnabled) {
            localStorage.removeItem('userInfo');
            ExternalRedirect(logoutURL);
        }
    };

    // Skip login screen if auth is disabled
    if (authEnabled && (user.isError || !user.data)) {
        localStorage.removeItem('userInfo');
        return <LoginScreen />;
    }

    const userData = authEnabled ? user.data : {
        sub: 'anonymous',
        email: 'anonymous@localhost',
        preferred_username: 'anonymous',
        name: 'Anonymous User',
        // ... other fields
    };

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

#### C. Conditional Navigation Elements
**File**: `jobmon_gui/src/components/navigation/PageNavigation.tsx`
```typescript
export default function PageNavigation({ children }: PropsWithChildren) {
    const { user } = useContext(AuthContext);
    const authEnabled = import.meta.env.VITE_APP_AUTH_ENABLED !== 'false';
    
    // ... existing code ...
    
    return (
        <Box sx={{ display: 'flex' }} id="AppBarBox">
            {/* ... existing AppBar code ... */}
            
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
        </Box>
    );
}
```

### 3. Configuration Management

#### A. Server Configuration Schema
**File**: Add to configuration schema or documentation
```ini
[auth]
# Enable/disable authentication system-wide
enabled = true

[oidc]
# OIDC settings (only used when auth.enabled = true)
name = "your_oidc_provider"
conf_url = "https://your-oidc-provider/.well-known/openid_configuration"
client_id = "your_client_id"
client_secret = "your_client_secret"
scope = "openid email profile"
admin_group = "admin"
```

#### B. Environment-based Configuration
**Docker Compose / Environment Variables**
```yaml
services:
  jobmon-server:
    environment:
      - JOBMON_AUTH__ENABLED=false
  
  jobmon-gui:
    environment:
      - VITE_APP_AUTH_ENABLED=false
```

### 4. Implementation Strategy

#### Phase 1: Server-Side Foundation
1. Add configuration option for auth toggle
2. Create conditional authentication dependencies
3. Modify API app setup to conditionally apply auth
4. Add anonymous user support

#### Phase 2: Frontend Adaptation  
1. Add environment variable for auth mode
2. Update AuthContext to support optional auth
3. Modify navigation components
4. Update API URL configurations

#### Phase 3: Testing & Documentation
1. Add unit tests for both auth modes
2. Integration tests for auth-disabled mode
3. Update deployment documentation
4. Create migration guide

### 5. Security Considerations

1. **Default Secure**: Authentication should be enabled by default
2. **Clear Indicators**: UI should clearly indicate when running in unauthenticated mode
3. **Feature Restrictions**: Some admin features might be disabled in unauthenticated mode
4. **Audit Logging**: Track when system runs in unauthenticated mode

### 6. Benefits

1. **Development Flexibility**: Easier local development without OIDC setup
2. **Deployment Options**: Can run in environments without identity providers
3. **Testing**: Simplified testing without authentication complexity
4. **Backwards Compatibility**: Existing authenticated deployments continue to work

### 7. Migration Path

For existing deployments:
1. No changes required (auth enabled by default)
2. Optional: Set `auth.enabled = false` to disable authentication
3. Update frontend environment variables if needed

This design maintains backward compatibility while providing the flexibility to run jobmon in environments where authentication is not required or available. 