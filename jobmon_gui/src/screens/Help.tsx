import React, { useEffect, useState } from 'react';
import ReactMarkdown from 'react-markdown';


export default function Help(){
    const [text, setText] = useState('')
    useEffect(()=>{
    const path = require("../assets/content/Help.md");

      fetch(path)
        .then(response => {
          return response.text()
        })
        .then(text => setText(text))
    },[])

    return(
        <div className="markdown-container">
            <ReactMarkdown>{text}</ReactMarkdown>
        </div>
    )
}