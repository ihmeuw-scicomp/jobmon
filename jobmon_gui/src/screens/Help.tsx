import ReactMarkdown from 'react-markdown';
import {markdown} from "@jobmon_gui/assets/content/Help.md"
import MarkdownLinkNewTabRenderer from '@jobmon_gui/utils/MarkdownLinkNewTabRender';
import { Box } from '@mui/material';
export default function Help(){
    return(
        <Box className="markdown-container">
            <ReactMarkdown components={{a: MarkdownLinkNewTabRenderer}}>{markdown}</ReactMarkdown>
        </Box>
    )
}