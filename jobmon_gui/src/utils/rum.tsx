import { apm, Transaction } from '@elastic/apm-rum';

export const get_rum_transaction = (name: string): Transaction | null => {
  const activeTransaction = apm.getCurrentTransaction();
  if (activeTransaction) {
    return activeTransaction;
  } else {
    //apm.setInitialPageLoadName(name);
    let t: Transaction = apm.startTransaction(name, 'custom');
    t.name = name;
    return t;
  }
};

export const init_apm = (pageloadname: string) => {
  try {
    let server_url = import.meta.env.VITE_APP_RUM_URL_DEV;
    if (window.location.origin === import.meta.env.VITE_APP_PROD_URL) {
      server_url = import.meta.env.VITE_APP_RUM_URL;
    }
    apm.init({
      serviceName: 'rum jobmon-gui',
      serverUrl: server_url,
      active: true,
      pageLoadTransactionName: pageloadname,
    });
    return apm;
  } catch (error) {
    console.log('Fail to initiate apm');
    console.log(error);
    return null;
  }
};

export const safe_rum_transaction = apm => {
  if (apm !== null && apm !== undefined) {
    return apm.getCurrentTransaction();
  } else {
    return null;
  }
};

export const safe_rum_add_label = (
  rum_obj: Transaction | null,
  key: string,
  value: string | number
) => {
  if (rum_obj !== null && rum_obj !== undefined) {
    rum_obj.addLabels({ [key]: value });
  }
};

export const safe_rum_start_span = (
  apm: typeof import('@elastic/apm-rum').apm | null,
  name: string,
  type: string
) => {
  if (apm !== null && apm !== undefined) {
    return apm.startSpan(name, type);
  } else {
    return null;
  }
};

export const safe_rum_unit_end = (rum_obj: Transaction | null) => {
  if (rum_obj !== null && rum_obj !== undefined) {
    return rum_obj.end();
  }
};
