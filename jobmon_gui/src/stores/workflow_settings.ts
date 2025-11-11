import { create } from 'zustand';
import { devtools, persist } from 'zustand/middleware';
import dayjs, { Dayjs } from 'dayjs';
import { settingsToSearchParamsString } from '@jobmon_gui/utils/workflowSearchParams';

export type WorkflowSearchSettings = {
    user: string;
    tool: string;
    wf_name: string;
    wf_args: string;
    wf_attribute_key: string;
    wf_attribute_value: string;
    wf_id: string;
    date_submitted: Dayjs;
    date_submitted_end: Dayjs;
    status: string;
};

const defaultSettings = {
    user: '',
    tool: '',
    wf_name: '',
    wf_args: '',
    wf_attribute_key: '',
    wf_attribute_value: '',
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
    setPendingSetting: (key: string, value: string) => void;
    getRefreshData: () => boolean;
    triggerDataRefresh: () => void;
    clearDataRefresh: () => void;
    updateUrlSearchParams: () => void;
    set: (newSettings: WorkflowSearchSettings) => void;
    setUser: (newValue: string) => void;
    setTool: (newValue: string) => void;
    setWfName: (newValue: string) => void;
    setWfArgs: (newValue: string) => void;
    setWfAttributeKey: (newValue: string) => void;
    setWfAttributeValue: (newValue: string) => void;
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
                setWfAttributeKey: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_attribute_key: newValue,
                        },
                    }));
                    get().updateUrlSearchParams();
                },
                setWfAttributeValue: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_attribute_value: newValue,
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
                    const loadedSettings = {
                        user: searchParams.get('user') || '',
                        tool: searchParams.get('tool') || '',
                        wf_name: searchParams.get('wf_name') || '',
                        wf_args: searchParams.get('wf_args') || '',
                        wf_attribute_key:
                            searchParams.get('wf_attribute_key') || '',
                        wf_attribute_value:
                            searchParams.get('wf_attribute_value') || '',
                        wf_id: searchParams.get('wf_id') || '',
                        date_submitted: searchParams.get('date_submitted')
                            ? dayjs(searchParams.get('date_submitted'))
                            : dayjs(),
                        date_submitted_end: searchParams.get(
                            'date_submitted_end'
                        )
                            ? dayjs(searchParams.get('date_submitted_end'))
                            : dayjs(),
                        status: searchParams.get('status') || '',
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
