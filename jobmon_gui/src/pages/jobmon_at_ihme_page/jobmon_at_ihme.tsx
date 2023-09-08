import React, { useEffect, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import '../../css/jobmon_gui.css'

const replacements = {
    "JOBMON_DB_HOST": process.env.REACT_APP_DOCS_DB_HOST,
    "JOBMON_DB_USER": process.env.REACT_APP_DOCS_DB_USER,
    "JOBMON_DB_PASSWORD": process.env.REACT_APP_DOCS_DB_PASSWORD,
    "JOBMON_DB_DATABASE": process.env.REACT_APP_DOCS_DB_DATABASE,
    "JOBMON_DB_PORT": process.env.REACT_APP_DOCS_DB_PORT,
}
export default function JobmonAtIHME(){
    const [text, setText] = useState('')
    let markdown;
    useEffect(()=>{
    const path = require("./jobmon_at_ihme.md");
      fetch(path)
        .then(response => {
          markdown = response.text()
          return markdown
        })
        .then(text => setText(text.replace(/JOBMON_DB_HOST|JOBMON_DB_USER|JOBMON_DB_PASSWORD|JOBMON_DB_DATABASE|JOBMON_DB_PORT/g,
        matched => replacements[matched])))
    },[])

    return(
        <div className="markdown-container">
            <ReactMarkdown>{text}</ReactMarkdown>
        </div>
    )
}