import React, { useEffect, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import '../../css/jobmon_gui.css'

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
        .then(text => setText(text.replace(/MY_VARIABLE/g, `${process.env.MY_VARIABLE}`)))
    },[])

    return(
        <div className="markdown-container">
            <ReactMarkdown>{text}</ReactMarkdown>
        </div>
    )
}