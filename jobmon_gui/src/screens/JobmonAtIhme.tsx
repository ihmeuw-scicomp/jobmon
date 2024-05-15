import React, { useEffect, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import '../styles/jobmon_gui.css'

const replacements = {
    "JOBMON_DB_HOST": process.env.REACT_APP_DOCS_DB_HOST,
    "JOBMON_DB_USER": process.env.REACT_APP_DOCS_DB_USER,
    "JOBMON_DB_PASSWORD": process.env.REACT_APP_DOCS_DB_PASSWORD,
    "JOBMON_DB_DATABASE": process.env.REACT_APP_DOCS_DB_DATABASE,
    "JOBMON_DB_PORT": process.env.REACT_APP_DOCS_DB_PORT,
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