import { create } from 'zustand';
import { devtools, persist } from 'zustand/middleware';
import dayjs, { Dayjs } from 'dayjs';
import {
    settingsToSearchParamsString,
    parseUrlFilterParam,
} from '@jobmon_gui/utils/workflowSearchParams';

export type FilterValue = {
    include?: string[];
    exclude?: string[];
};

export type WfAttributePair = {
    key: string;
    value: string;
};

export type WorkflowSearchSettings = {
    user: string | FilterValue;
    tool: string | FilterValue;
    wf_name: string;
    wf_args: string;
    wf_name_contains: boolean;
    wf_args_contains: boolean;
    wf_attributes: WfAttributePair[];
    wf_id: string;
    date_submitted: Dayjs;
    date_submitted_end: Dayjs;
    status: string | FilterValue;
};

const defaultSettings: WorkflowSearchSettings = {
    user: '',
    tool: '',
    wf_name: '',
    wf_args: '',
    wf_name_contains: false,
    wf_args_contains: false,
    wf_attributes: [{ key: '', value: '' }],
    wf_id: '',
    date_submitted: dayjs(),
    date_submitted_end: dayjs(),
    status: '',
};

export type WorkflowSearchSettingsStore = {
    settings: WorkflowSearchSettings;
    pendingSettings: WorkflowSearchSettings;
    refreshData: boolean;
    applyPendingSettings: () => void;
    resetPendingSettings: () => void;
    setPendingSetting: <K extends keyof WorkflowSearchSettings>(
        key: K,
        value: WorkflowSearchSettings[K]
    ) => void;
    getRefreshData: () => boolean;
    triggerDataRefresh: () => void;
    clearDataRefresh: () => void;
    updateUrlSearchParams: () => void;
    set: (newSettings: WorkflowSearchSettings) => void;
    setUser: (newValue: string) => void;
    setTool: (newValue: string) => void;
    setWfName: (newValue: string) => void;
    setWfArgs: (newValue: string) => void;
    setWfId: (newValue: string) => void;
    setDateSubmitted: (newValue: Dayjs) => void;
    setDateSubmittedEnd: (newValue: Dayjs) => void;
    setStatus: (newValue: string) => void;
    get: () => WorkflowSearchSettings;
    getPending: () => WorkflowSearchSettings;
    loadValuesFromSearchParams: (searchParams: URLSearchParams) => void;
    clear: () => void;
};

export const useWorkflowSearchSettings = create<WorkflowSearchSettingsStore>()(
    devtools(
        persist(
            (set, get) => ({
                settings: defaultSettings,
                pendingSettings: defaultSettings,
                refreshData: false,
                applyPendingSettings: () => {
                    set({
                        settings: get().pendingSettings,
                        refreshData: true,
                    });
                },
                resetPendingSettings: () =>
                    set({ pendingSettings: defaultSettings }),
                setPendingSetting: (key, value) =>
                    set(state => ({
                        pendingSettings: {
                            ...state.pendingSettings,
                            [key]: value,
                        },
                    })),
                triggerDataRefresh: () => set({ ...get(), refreshData: true }),
                clearDataRefresh: () => set({ ...get(), refreshData: false }),
                getRefreshData: () => get().refreshData,
                updateUrlSearchParams: () => {
                    const searchString = settingsToSearchParamsString(
                        get().settings
                    );
                    const currentHash = window.location.hash;
                    const hashPath = currentHash.includes('?')
                        ? currentHash.split('?')[0]
                        : currentHash || '#/';
                    const newHash = searchString
                        ? `${hashPath}?${searchString}`
                        : hashPath;

                    if (window.location.hash !== newHash) {
                        window.location.hash = newHash;
                    }
                },
                set: (newSettings: WorkflowSearchSettings) =>
                    set(() => ({ settings: newSettings })),
                setUser: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            user: newValue,
                        },
                    }));
                    get().updateUrlSearchParams();
                },
                setTool: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            tool: newValue,
                        },
                    }));
                    get().updateUrlSearchParams();
                },
                setWfName: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_name: newValue,
                        },
                    }));
                    get().updateUrlSearchParams();
                },
                setWfArgs: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_args: newValue,
                        },
                    }));

                    get().updateUrlSearchParams();
                },
                setWfId: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_id: newValue,
                        },
                    }));
                    get().updateUrlSearchParams();
                },
                setDateSubmitted: (newValue: Dayjs) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            date_submitted: dayjs(newValue),
                        },
                    }));
                    get().updateUrlSearchParams();
                },
                setDateSubmittedEnd: (newValue: Dayjs) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            date_submitted_end: dayjs(newValue),
                        },
                    }));
                    get().updateUrlSearchParams();
                },
                setStatus: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            status: newValue,
                        },
                    }));
                    get().updateUrlSearchParams();
                },
                get: () => get().settings,
                getPending: () => get().pendingSettings,
                loadValuesFromSearchParams: (searchParams: URLSearchParams) => {
                    // Parse wf_attr repeated params into pairs
                    const wfAttrParams = searchParams.getAll('wf_attr');
                    let wf_attributes: WfAttributePair[] = [
                        { key: '', value: '' },
                    ];
                    if (wfAttrParams.length > 0) {
                        wf_attributes = wfAttrParams
                            .filter(p => p.includes(':'))
                            .map(p => {
                                const [k, ...rest] = p.split(':');
                                return {
                                    key: k.trim(),
                                    value: rest.join(':').trim(),
                                };
                            });
                        if (wf_attributes.length === 0) {
                            wf_attributes = [{ key: '', value: '' }];
                        }
                    }

                    const loadedSettings: WorkflowSearchSettings = {
                        user: parseUrlFilterParam(searchParams, 'user'),
                        tool: parseUrlFilterParam(searchParams, 'tool'),
                        wf_name: searchParams.get('wf_name') || '',
                        wf_args: searchParams.get('wf_args') || '',
                        wf_name_contains:
                            searchParams.get('wf_name_contains') === 'true',
                        wf_args_contains:
                            searchParams.get('wf_args_contains') === 'true',
                        wf_attributes,
                        wf_id: searchParams.get('wf_id') || '',
                        date_submitted: searchParams.get('date_submitted')
                            ? dayjs(searchParams.get('date_submitted'))
                            : dayjs(),
                        date_submitted_end: searchParams.get(
                            'date_submitted_end'
                        )
                            ? dayjs(searchParams.get('date_submitted_end'))
                            : dayjs(),
                        status: parseUrlFilterParam(searchParams, 'status'),
                    };
                    set({
                        settings: loadedSettings,
                        pendingSettings: loadedSettings,
                    });
                },
                clear: () => {
                    set(() => ({
                        settings: defaultSettings,
                        pendingSettings: defaultSettings,
                    }));
                    get().updateUrlSearchParams();
                },
            }),
            {
                name: 'WorkflowSearchSettings',
            }
        )
    )
);
