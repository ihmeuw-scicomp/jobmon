export const convertDate = (date: string) => {
        let raw = new Date((typeof date === "string" ? new Date(date) : date));
        return new Date(raw.getTime() + raw.getTimezoneOffset()*60*1000)
}

export const convertDatePST = (date: string) => {
    const converted_date = convertDate(date)
    return converted_date.toLocaleString("en-US", { timeZone: "America/Los_Angeles", dateStyle: "full", timeStyle: "long" })
}

export const formatNumber = (x) => {
       if (x){
        return x.toLocaleString()
    }
    return x
}

export const formatBytes = (x) => {
       const units = ['bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
       let l = 0, n = parseInt(x, 10) || 0;

       while(n >= 1024 && ++l){
           n = n/1024;
       }

       return(n.toFixed(n < 10 && l > 0 ? 1 : 0) + ' ' + units[l]);
}

export const bytes_to_gib = (x) => {
    if (x){
        return x/1073741824
    }
    return x
}
