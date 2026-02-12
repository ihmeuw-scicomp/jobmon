import React, { PropsWithChildren } from 'react';
import {
    createTheme,
    responsiveFontSizes,
    ThemeProvider,
} from '@mui/material/styles';
import '@fontsource/archivo';

declare module '@mui/material/styles' {
    interface TypographyVariants {
        banner: React.CSSProperties;
    }

    // allow configuration using `createTheme`
    interface TypographyVariantsOptions {
        banner?: React.CSSProperties;
    }
}

// Update the Typography's variant prop options
declare module '@mui/material/Typography' {
    interface TypographyPropsVariantOverrides {
        banner: true;
    }
}

const CustomThemeProvider = ({ children }: PropsWithChildren) => {
    let theme = createTheme({
        palette: {
            primary: {
                main: '#17B9CF',
                light: '#FFFEFC',
                dark: '#02262E',
                contrastText: '#fff',
            },
            secondary: {
                main: '#32CA81',
                light: '#89FFA8',
                dark: '#073320',
            },
            text: {
                primary: 'rgba(77,77,79,1)',
                secondary: 'rgba(0,0,0)',
            },
            error: {
                light: '#FED3C6',
                main: '#FF8B66',
                dark: '#560E0B',
            },
            warning: {
                light: '#faab52',
                main: '#e28126',
                dark: '#ab6f2c',
            },
            info: {
                light: '#56c8e0',
                main: '#279cba',
                dark: '#006f8c',
            },
            success: {
                light: '#c4dc78',
                main: '#9dcb3b',
                dark: '#5d833a',
            },
        },
        typography: {
            fontFamily: [
                'Archivo',
                '-apple-system',
                'BlinkMacSystemFont',
                'Segoe UI',
                'Roboto',
                'Oxygen',
                'Ubuntu',
                'Cantarell',
                'Fira Sans',
                'Droid Sans',
                'Helvetica Neue',
                'sans-serif',
            ].join(','),
            banner: {
                fontFamily: 'Archivo',
                fontWeight: 300,
                fontSize: '1.25rem',
                lineHeight: 1.2,
                letterSpacing: '0em',
                color: 'white',
            },
        },
    });
    theme = createTheme(theme, {
        components: {
            MuiButton: {
                styleOverrides: {
                    root: {
                        fontWeight: 'bold',
                    },
                },
            },
        },
    });

    theme = responsiveFontSizes(theme);

    return <ThemeProvider theme={theme}>{children}</ThemeProvider>;
};

export default CustomThemeProvider;
