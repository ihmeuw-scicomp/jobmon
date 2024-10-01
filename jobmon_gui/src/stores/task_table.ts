import {create} from 'zustand'
import {devtools, persist} from 'zustand/middleware'


export type TaskTableFilter = {
    id: string
    value: string
}

export type TaskTableColumnsStore = {
    columnFilters: TaskTableFilter[]
    set: (newFilters: TaskTableFilter[]) => void
    get: () => TaskTableFilter[]
    clear: () => void
}


export const useTaskTableColumnsStore = create<TaskTableColumnsStore>()(
    devtools(
        (set, get) => ({
            columnFilters: [],
            set: (newFilters: TaskTableFilter[]) => set(() => ({columnFilters: newFilters})),
            get: () => get().columnFilters,
            clear: () => {
                set(() => ({columnFilters: []}))
            }
        }),
    ),
)