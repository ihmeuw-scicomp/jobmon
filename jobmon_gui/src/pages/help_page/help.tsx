import React, { useEffect, useState } from 'react';
import ReactMarkdown from 'react-markdown';


export default function Help(){
    const [text, setText] = useState('')
    useEffect(()=>{
    const path = require("./help.md");

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