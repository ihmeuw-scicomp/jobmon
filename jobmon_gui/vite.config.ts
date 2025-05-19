import {defineConfig} from "vite"
import react from "@vitejs/plugin-react"
import path from "path";
import mdPlugin, {Mode} from "vite-plugin-markdown"
// https://vitejs.dev/config/
export default defineConfig({
    base: '/',
    plugins: [react(), mdPlugin({mode: [Mode.MARKDOWN]})],
    server: {
        host: '0.0.0.0', // Allows access from external IPs
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
