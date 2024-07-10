import {create} from 'zustand'
import {createJSONStorage, devtools, persist} from 'zustand/middleware'
import dayjs, {Dayjs} from "dayjs"; // required for devtools typing

const getUrlSearch = () => {
    return window.location.search.slice(1)
}


export type WorkflowSearchSettings = {
    user: string
    tool: string
    wf_name: string
    wf_args: string
    wf_attribute: string
    wf_id: string
    date_submitted: Dayjs
    "status": string
}


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
    clear: () => void
}

export const useWorkflowSearchSettings = create<WorkflowSearchSettingsStore>()(
    devtools(
        persist(
            (set, get) => ({
                settings: defaultSettings,
                set: (newSettings: WorkflowSearchSettings) => set(() => ({settings: newSettings})),
                setUser: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            user: newValue
                        }
                    }))

                    const searchParams = new URLSearchParams(getUrlSearch())
                    searchParams.set("user", JSON.stringify(newValue))
                },
                setTool: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            tool: newValue
                        }
                    }))

                    const searchParams = new URLSearchParams(getUrlSearch())
                    searchParams.set("tool", JSON.stringify(newValue))
                },
                setWfName: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_name: newValue
                        }
                    }))

                    const searchParams = new URLSearchParams(getUrlSearch())
                    searchParams.set("wf_name", JSON.stringify(newValue))
                },
                setWfArgs: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_args: newValue
                        }
                    }))

                    const searchParams = new URLSearchParams(getUrlSearch())
                    searchParams.set("wf_args", JSON.stringify(newValue))
                },
                setWfAttributeKey: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_attribute_key: newValue
                        }
                    }))
                    const searchParams = new URLSearchParams(getUrlSearch())
                    searchParams.set("wf_attribute_key", JSON.stringify(newValue))
                },
                setWfAttributeValue: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_attribute_value: newValue
                        }
                    }))
                    const searchParams = new URLSearchParams(getUrlSearch())
                    searchParams.set("wf_attribute_value", JSON.stringify(newValue))
                },
                setWfId: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            wf_id: newValue
                        }
                    }))
                    const searchParams = new URLSearchParams(getUrlSearch())
                    searchParams.set("wf_id", JSON.stringify(newValue))
                },
                setDateSubmitted: (newValue: Dayjs) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            date_submitted: newValue
                        }
                    }))
                    const searchParams = new URLSearchParams(getUrlSearch())
                    searchParams.set("date_submitted", JSON.stringify(newValue))
                },
                setStatus: (newValue: string) => {
                    set(() => ({
                        settings: {
                            ...get().settings,
                            status: newValue
                        }
                    }))
                    const searchParams = new URLSearchParams(getUrlSearch())
                    searchParams.set("status", JSON.stringify(newValue))

                },
                get: () => {
                    if (getUrlSearch()) {
                        const searchParams = new URLSearchParams(getUrlSearch())
                        const urlUser = JSON.parse(searchParams.get("user"))
                        const urlTool = JSON.parse(searchParams.get("tool"))
                        const urlWfName = JSON.parse(searchParams.get("wf_name"))
                        const urlWfArgs = JSON.parse(searchParams.get("wf_args"))
                        const urlWfAttributeKey = JSON.parse(searchParams.get("wf_attribute_key"))
                        const urlWfAttributeValue = JSON.parse(searchParams.get("wf_attribute_value"))
                        const urlWfId = JSON.parse(searchParams.get("wf_id"))
                        const urlDateSubmitted = dayjs(JSON.parse(searchParams.get("date_submitted")))
                        const urlStatus = JSON.parse(searchParams.get("status"))


                        set(() => ({
                            settings: {
                                user: urlUser,
                                tool: urlTool,
                                wf_name: urlWfName,
                                wf_args: urlWfArgs,
                                wf_attribute_key: urlWfAttributeKey,
                                wf_attribute_value: urlWfAttributeValue,
                                wf_id: urlWfId,
                                date_submitted: urlDateSubmitted,
                                status: urlStatus,
                            }
                        }))

                    }
                    return get().settings


                },
                clear: () => set(() => ({settings: defaultSettings}))
            }),
            {
                name: 'WorkflowSearchSettings',
                storage: createJSONStorage<WorkflowSearchSettingsStore>(() => useWorkflowSearchSettings),
            }
            ,
        ),
    ),
)