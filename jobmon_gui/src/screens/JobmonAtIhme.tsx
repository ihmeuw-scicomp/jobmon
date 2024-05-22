import React, { useEffect, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import '../styles/jobmon_gui.css'

const replacements = {
    "JOBMON_DB_HOST": import.meta.env.VITE_APP_DOCS_DB_HOST,
    "JOBMON_DB_USER": import.meta.env.VITE_APP_DOCS_DB_USER,
    "JOBMON_DB_PASSWORD": import.meta.env.VITE_APP_DOCS_DB_PASSWORD,
    "JOBMON_DB_DATABASE": import.meta.env.VITE_APP_DOCS_DB_DATABASE,
    "JOBMON_DB_PORT": import.meta.env.VITE_APP_DOCS_DB_PORT,
}
export default function JobmonAtIHME(){
    const [text, setText] = useState('')
    useEffect(() => {
        const path = require("../assets/content/JobmonAtIhme.md");
        fetch(path)
            .then(response => {
                return response.text();
            })
            .then(text => {
                const markdown = text.replace(/JOBMON_DB_HOST|JOBMON_DB_USER|JOBMON_DB_PASSWORD|JOBMON_DB_DATABASE|JOBMON_DB_PORT/g,
                    matched => replacements[matched]);
                setText(markdown);
            });
    }, []);

    return(
        <div className="markdown-container">
            <ReactMarkdown>{text}</ReactMarkdown>
        </div>
    )
}