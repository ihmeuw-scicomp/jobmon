import ReactMarkdown from 'react-markdown';
import { Box } from "@mui/material";
import Mustache from 'mustache';
import MarkdownLinkNewTabRenderer from "@jobmon_gui/utils/MarkdownLinkNewTabRender";
import { markdown } from "@jobmon_gui/assets/content/JobmonAtIhme.md";

const templateData = {
    JOBMON_DB_HOST: import.meta.env.VITE_APP_DOCS_DB_HOST,
    JOBMON_DB_USER: import.meta.env.VITE_APP_DOCS_DB_USER,
    JOBMON_DB_PASSWORD: import.meta.env.VITE_APP_DOCS_DB_PASSWORD,
    JOBMON_DB_DATABASE: import.meta.env.VITE_APP_DOCS_DB_DATABASE,
    JOBMON_DB_PORT: import.meta.env.VITE_APP_DOCS_DB_PORT,
};

export default function JobmonAtIHME() {
    const templatedMarkdown = Mustache.render(markdown, templateData);

    return (
        <Box className="markdown-container">
            <ReactMarkdown components={{ a: MarkdownLinkNewTabRenderer }}>
                {templatedMarkdown}
            </ReactMarkdown>
        </Box>
    );
}
