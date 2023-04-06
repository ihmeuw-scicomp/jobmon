import { apm } from '@elastic/apm-rum'


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

export const get_rum_transaction = (name) => {
    const activeTransaction = apm.getCurrentTransaction();
    if (activeTransaction) {
        return activeTransaction;
    }else{
        //apm.setInitialPageLoadName(name);
        let t: any = apm.startTransaction(name, "custom");
        t.name = name;
        return t;
    }
}

export const init_apm = (pageloadname) => {
     try{
        let server_url = process.env.REACT_APP_RUM_URL_DEV;
        if (window.location.origin === process.env.REACT_APP_PROD_URL) {
            server_url = process.env.REACT_APP_RUM_URL;
        }
        apm.init({
            serviceName: "rum jobmon-gui",
            serverUrl: server_url,
            active: true,
            pageLoadTransactionName: pageloadname,
        })
        return apm;
    }catch(error){
        console.log("Fail to initiate apm");
        console.log(error);
        return null;
    }
}

export const safe_rum_transaction = (apm) => {
    if (apm !== null && apm != undefined){
        return apm.getCurrentTransaction();
    }
    else {
        return null;
    }
}

export const safe_rum_add_label = (rum_obj, key, value) => {
    if (rum_obj !== null && rum_obj !== undefined){
        rum_obj.addLabels({key: value});
    }
}

export const safe_rum_start_span = (apm, name, type) => {
    if (apm !== null && apm != undefined){
        return apm.startSpan(name, type);
    }else{
        return null;
    }
}

export const safe_rum_unit_end = (rum_obj) => {
    if (rum_obj != null && rum_obj != undefined){
        return rum_obj.end();
    }
}
