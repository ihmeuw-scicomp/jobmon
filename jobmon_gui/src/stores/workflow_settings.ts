import {create} from 'zustand'
import {createJSONStorage, devtools, persist} from 'zustand/middleware'
import dayjs, {Dayjs} from "dayjs"; // required for devtools typing

const getUrlSearch = () => {
    return window.location.search.substring(1)
}


export type WorkflowSearchSettings = {
    user: string
    tool: string
    wf_name: string
    wf_args: string
    wf_attribute_key: string,
    wf_attribute_value: string,
    wf_id: string
    date_submitted: Dayjs
    "status": string
}

type WorkflowSearchSettingsSearchParams = Omit<WorkflowSearchSettings, 'date_submitted'> & { date_submitted: string };


const twoWeeksMs = 12096e5; // 2 weeks in milliseconds
const twoWeeksAgo = new Date(Date.now() - twoWeeksMs);
const defaultSettings = {
    user: "",
    tool: "",
    wf_name: "",
    wf_args: "",
    wf_attribute_key: "",
    wf_attribute_value: "",
    wf_id: "",
    date_submitted: dayjs().subtract(2, "weeks"),
    "status": "",
}

export type WorkflowSearchSettingsStore = {
    settings: WorkflowSearchSettings
    refreshData: boolean
    getRefreshData: () => boolean
    triggerDataRefresh: () => void
    clearDataRefresh: () => void
    updateUrlSearchParams: () => void
    set: (newSettings: WorkflowSearchSettings) => void
    setUser: (newValue: string) => void
    setTool: (newValue: string) => void
    setWfName: (newValue: string) => void
    setWfArgs: (newValue: string) => void
    setWfAttributeKey: (newValue: string) => void
    setWfAttributeValue: (newValue: string) => void
    setWfId: (newValue: string) => void
    setDateSubmitted: (newValue: Dayjs) => void
    setStatus: (newValue: string) => void
    get: () => WorkflowSearchSettings
    loadValuesFromSearchParams: (searchParams: URLSearchParams) => void
    clear: () => void
}

export const useWorkflowSearchSettings = create<WorkflowSearchSettingsStore>()(
    devtools(
        persist(
            (set, get) => ({
                settings: defaultSettings,
                refreshData: false,
                triggerDataRefresh: () => set({...get(), refreshData: true}),
                clearDataRefresh: () => set({...get(), refreshData: false}),
                getRefreshData: () => get().refreshData,
                updateUrlSearchParams: () => {
                    const currentSettings = get().settings
                    const date_submitted = dayjs(currentSettings?.date_submitted).format("YYYY-MM-DD") || dayjs().format("YYYY-MM-DD")

                    const searchParams = new URLSearchParams({
                        user: currentSettings.user,
                        tool: currentSettings.tool,
                        wf_name: currentSettings.wf_name,
                        wf_args: currentSettings.wf_args,
                        wf_attribute_key: currentSettings.wf_attribute_key,
                        wf_attribute_value: currentSettings.wf_attribute_value,
                        wf_id: currentSettings.wf_id,
                        date_submitted: date_submitted,
                        "status": currentSettings.status,
                    })
                    location.hash = "?" + searchParams.toString()

                },
                set: (newSettings: WorkflowSearchSettings) => set(() => ({settings: newSettings})),
                setUser: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            user: newValue
                        }
                    }))
                    get().updateUrlSearchParams()
                },
                setTool: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            tool: newValue
                        }
                    }))

                    get().updateUrlSearchParams()
                },
                setWfName: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_name: newValue
                        }
                    }))

                    get().updateUrlSearchParams()
                },
                setWfArgs: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_args: newValue
                        }
                    }))

                    get().updateUrlSearchParams()
                },
                setWfAttributeKey: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_attribute_key: newValue
                        }
                    }))
                    get().updateUrlSearchParams()
                },
                setWfAttributeValue: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_attribute_value: newValue
                        }
                    }))
                    get().updateUrlSearchParams()
                },
                setWfId: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_id: newValue
                        }
                    }))
                    get().updateUrlSearchParams()
                },
                setDateSubmitted: (newValue: Dayjs) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            date_submitted: dayjs(newValue)
                        }
                    }))
                    get().updateUrlSearchParams()
                },
                setStatus: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            status: newValue
                        }
                    }))
                    get().updateUrlSearchParams()

                },
                get: () => get().settings,
                loadValuesFromSearchParams: (searchParams: URLSearchParams) => {
                    const urlUser = searchParams.get("user") || ""
                    const urlTool = searchParams.get("tool") || ""
                    const urlWfName = searchParams.get("wf_name") || ""
                    const urlWfArgs = searchParams.get("wf_args") || ""
                    const urlWfAttributeKey = searchParams.get("wf_attribute_key") || ""
                    const urlWfAttributeValue = searchParams.get("wf_attribute_value") || ""
                    const urlWfId = searchParams.get("wf_id") || ""
                    const urlDateSubmitted = searchParams.get("date_submitted") ? dayjs(searchParams.get("date_submitted")) : dayjs().subtract(2, 'weeks')
                    const urlStatus = searchParams.get("status") || ""


                    set({
                        settings: {
                            user: urlUser,
                            tool: urlTool,
                            wf_name: urlWfName,
                            wf_args: urlWfArgs,
                            wf_attribute_key: urlWfAttributeKey,
                            wf_attribute_value: urlWfAttributeValue,
                            wf_id: urlWfId,
                            date_submitted: urlDateSubmitted || dayjs().subtract(2, 'weeks'),
                            status: urlStatus,
                        }
                    })

                },
                clear: () => {
                    set(() => ({settings: defaultSettings}))
                    get().updateUrlSearchParams()
                }
            }),
            {
                name: 'WorkflowSearchSettings',
            }
            ,
        ),
    ),
)