import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import axios from 'axios';
import { AuthProvider } from '../contexts/AuthContext';
import { describe, it, beforeEach, expect, vi } from 'vitest';

// Mock axios
vi.mock('axios');
const mockedAxios = axios as any;

// Mock Vite environment variables
const mockEnv = (authEnabled: string | undefined = 'true') => {
    vi.stubEnv('VITE_APP_AUTH_ENABLED', authEnabled);
};

// Test component that consumes AuthContext
const TestComponent = () => {
    return <div data-testid="test-component">Test Content</div>;
};

describe('AuthContext', () => {
    let queryClient: QueryClient;

    beforeEach(() => {
        queryClient = new QueryClient({
            defaultOptions: {
                queries: {
                    retry: false,
                },
            },
        });
        vi.clearAllMocks();
        vi.unstubAllEnvs();
    });

    describe('when authentication is enabled', () => {
        beforeEach(() => {
            mockEnv('true');
        });

        it('should fetch user info when auth is enabled and succeed', async () => {
            const mockUser = {
                sub: 'user123',
                email: 'test@example.com',
                preferred_username: 'testuser',
                name: 'Test User',
                updated_at: 123456789,
                given_name: 'Test',
                family_name: 'User',
                nonce: 'nonce123',
                at_hash: 'hash123',
                sid: 'session123',
                aud: 'audience',
                exp: 999999999,
                iat: 123456789,
                iss: 'issuer',
                is_admin: false,
            };

            mockedAxios.get.mockResolvedValue({ data: mockUser });

            render(
                <QueryClientProvider client={queryClient}>
                    <AuthProvider>
                        <TestComponent />
                    </AuthProvider>
                </QueryClientProvider>
            );

            await waitFor(() => {
                expect(
                    screen.getByTestId('test-component')
                ).toBeInTheDocument();
            });

            expect(mockedAxios.get).toHaveBeenCalledWith(
                expect.stringContaining('/api/auth/oidc/userinfo'),
                expect.any(Object)
            );
        });

        it('should show login screen when auth is enabled and user fetch fails', async () => {
            mockedAxios.get.mockRejectedValue(new Error('Unauthorized'));

            render(
                <QueryClientProvider client={queryClient}>
                    <AuthProvider>
                        <TestComponent />
                    </AuthProvider>
                </QueryClientProvider>
            );

            await waitFor(() => {
                expect(
                    screen.queryByTestId('test-component')
                ).not.toBeInTheDocument();
            });
        });
    });

    describe('when authentication is disabled', () => {
        beforeEach(() => {
            mockEnv('false');
        });

        it('should return anonymous user when auth is disabled', async () => {
            render(
                <QueryClientProvider client={queryClient}>
                    <AuthProvider>
                        <TestComponent />
                    </AuthProvider>
                </QueryClientProvider>
            );

            await waitFor(() => {
                expect(
                    screen.getByTestId('test-component')
                ).toBeInTheDocument();
            });

            // Should not make API calls when auth is disabled
            expect(mockedAxios.get).not.toHaveBeenCalled();
        });

        it('should not show login screen when auth is disabled', async () => {
            render(
                <QueryClientProvider client={queryClient}>
                    <AuthProvider>
                        <TestComponent />
                    </AuthProvider>
                </QueryClientProvider>
            );

            await waitFor(() => {
                expect(
                    screen.getByTestId('test-component')
                ).toBeInTheDocument();
            });

            // Login screen should not be present
            expect(screen.queryByText(/login/i)).not.toBeInTheDocument();
        });
    });

    describe('environment variable edge cases', () => {
        it('should treat undefined VITE_APP_AUTH_ENABLED as enabled (default)', async () => {
            mockEnv(undefined);

            const mockUser = {
                sub: 'user123',
                email: 'test@example.com',
                preferred_username: 'testuser',
                name: 'Test User',
                updated_at: 123456789,
                given_name: 'Test',
                family_name: 'User',
                nonce: 'nonce123',
                at_hash: 'hash123',
                sid: 'session123',
                aud: 'audience',
                exp: 999999999,
                iat: 123456789,
                iss: 'issuer',
                is_admin: false,
            };

            mockedAxios.get.mockResolvedValue({ data: mockUser });

            render(
                <QueryClientProvider client={queryClient}>
                    <AuthProvider>
                        <TestComponent />
                    </AuthProvider>
                </QueryClientProvider>
            );

            await waitFor(() => {
                expect(
                    screen.getByTestId('test-component')
                ).toBeInTheDocument();
            });

            expect(mockedAxios.get).toHaveBeenCalled();
        });

        it('should treat empty string as enabled', async () => {
            mockEnv('');

            const mockUser = {
                sub: 'user123',
                email: 'test@example.com',
                preferred_username: 'testuser',
                name: 'Test User',
                updated_at: 123456789,
                given_name: 'Test',
                family_name: 'User',
                nonce: 'nonce123',
                at_hash: 'hash123',
                sid: 'session123',
                aud: 'audience',
                exp: 999999999,
                iat: 123456789,
                iss: 'issuer',
                is_admin: false,
            };

            mockedAxios.get.mockResolvedValue({ data: mockUser });

            render(
                <QueryClientProvider client={queryClient}>
                    <AuthProvider>
                        <TestComponent />
                    </AuthProvider>
                </QueryClientProvider>
            );

            await waitFor(() => {
                expect(
                    screen.getByTestId('test-component')
                ).toBeInTheDocument();
            });

            expect(mockedAxios.get).toHaveBeenCalled();
        });
    });
});
