import React, { useEffect, useState } from 'react';
import ReactMarkdown from 'react-markdown';


export default function JobmonAtIHME(){
    const [text, setText] = useState('')
    useEffect(()=>{
    const path = require("./jobmon_at_ihme.md");

      fetch(path)
        .then(response => {
          return response.text()
        })
        .then(text => setText(text))
    },[])

    return(
        <div>
            <ReactMarkdown>{text}</ReactMarkdown>
        </div>
    )
}