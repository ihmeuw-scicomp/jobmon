/// <reference types="@elastic/apm-rum" />
export declare const convertDate: (date: string) => Date;
export declare const convertDatePST: (date: string) => string;
export declare const formatNumber: (x: any) => any;
export declare const formatBytes: (x: any) => string;
export declare const bytes_to_gib: (x: any) => any;
export declare const get_rum_transaction: (name: any) => any;
export declare const init_apm: (pageloadname: any) => {
    serviceFactory: ServiceFactory;
    init: (options?: import("@elastic/apm-rum").AgentConfigOptions | undefined) => any;
    isEnabled(): boolean;
    isActive(): boolean;
    observe(name: TransactionEvents, callback: (tr: import("@elastic/apm-rum").Transaction) => void): void;
    config(config: import("@elastic/apm-rum").AgentConfigOptions): void;
    setUserContext(user: UserObject): void;
    setCustomContext(custom: object): void;
    addLabels(labels: Labels): void;
    setInitialPageLoadName(name: string): void;
    startTransaction(name?: string | null | undefined, type?: string | null | undefined, options?: import("@elastic/apm-rum").TransactionOptions | undefined): import("@elastic/apm-rum").Transaction | undefined;
    startSpan(name?: string | null | undefined, type?: string | null | undefined, options?: import("@elastic/apm-rum").SpanOptions | undefined): import("@elastic/apm-rum").Span | undefined;
    getCurrentTransaction(): import("@elastic/apm-rum").Transaction | undefined;
    captureError(error: string | Error): void;
    addFilter(fn: FilterFn): void;
} | null;
export declare const safe_rum_transaction: (apm: any) => any;
export declare const safe_rum_add_label: (rum_obj: any, key: any, value: any) => void;
export declare const safe_rum_start_span: (apm: any, name: any, type: any) => any;
export declare const safe_rum_unit_end: (rum_obj: any) => any;
