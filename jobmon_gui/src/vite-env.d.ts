/// <reference types="vite/client" />
declare const APP_VERSION: string;

declare module 'plotly.js-cartesian-dist' {
    import Plotly from 'plotly.js';
    export default Plotly;
}
