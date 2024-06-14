import {defineConfig} from 'vite'
import react from '@vitejs/plugin-react'
import path from "path";

// https://vitejs.dev/config/
export default defineConfig({
    base: '/',
    plugins: [react()],
    server: {
        port: 3000,
    },
    define: {
        APP_VERSION: JSON.stringify(process.env.npm_package_version),
    },
    resolve: {
        alias: {
            '@jobmon_gui': path.resolve(__dirname, './src'),
        },
    },
})
