import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import {
    PaginationState,
    Updater,
    SortingState,
    ColumnOrderState,
    VisibilityState,
} from '@tanstack/react-table';
import { MRT_DensityState } from 'material-react-table';
import React from 'react';
import dayjs, { Dayjs } from 'dayjs';
import { createJSONStorage } from 'zustand/middleware';

export const job_info_table_time_column_names = ['task_status_date'];

const taskTableStorage = createJSONStorage(() => localStorage, {
    reviver: (key, value) => {
        if (key == 'pageIndex') {
            return 0;
        }
        if (
            key == 'value' &&
            Array.isArray(value) &&
            value.length == 2 &&
            (dayjs(value[0]).isValid() || dayjs(value[1]).isValid())
        ) {
            return [
                value[0] ? dayjs(value[0]) : '',
                value[1] ? dayjs(value[1]) : '',
            ];
        }
        return value;
    },
});

export interface ColumnFilter {
    id: string;
    value: string | number | Dayjs[] | unknown;
}

export type ColumnFiltersState = ColumnFilter[];

export type TaskTableStore = {
    maxRows: number;
    setMaxRows: (newMaxRows: number) => void;
    getMaxRows: () => number;

    pagination: PaginationState;
    getPagination: () => PaginationState;
    setPagination: React.Dispatch<React.SetStateAction<PaginationState>>;

    filters: ColumnFiltersState;
    getFilters: () => ColumnFiltersState;
    setFilters: React.Dispatch<React.SetStateAction<ColumnFiltersState>>;

    sorting: SortingState;
    getSorting: () => SortingState;
    setSorting: React.Dispatch<React.SetStateAction<SortingState>>;

    columnOrder: ColumnOrderState;
    getColumnOrder: () => ColumnOrderState;
    setColumnOrder: React.Dispatch<React.SetStateAction<ColumnOrderState>>;

    density: MRT_DensityState;
    getDensity: () => MRT_DensityState;
    setDensity: React.Dispatch<React.SetStateAction<MRT_DensityState>>;

    columnVisibility: VisibilityState;
    getColumnVisibility: () => VisibilityState;
    setColumnVisibility: React.Dispatch<React.SetStateAction<VisibilityState>>;

    filterVisibility: boolean;
    getFilterVisibility: () => boolean;
    setFilterVisibility: React.Dispatch<React.SetStateAction<boolean>>;
};

const dateRangeFilter = (f: ColumnFilter) => {
    if (job_info_table_time_column_names.includes(f.id)) {
        if (
            Array.isArray(f.value) &&
            f.value.length == 2 &&
            (dayjs(f.value[0]).isValid() || dayjs(f.value[1]).isValid())
        ) {
            const range_start = f.value[0] ? dayjs(f.value[0]) : '';
            const range_end = f.value[1] ? dayjs(f.value[1]) : '';

            if (range_start && range_end && range_start.diff(range_end) > 0) {
                return {
                    id: f.id,
                    value: [f.value[0], f.value[0].add(1, 'day')],
                };
            }
        }
    }
    return f;
};

/** Factory for zustand setters that accept Updater<T>. */
function zustandSetter<S, K extends keyof S>(
    get: () => S,
    set: (partial: Partial<S>) => void,
    key: K,
    transform?: (val: S[K]) => S[K]
): (updaterOrValue: Updater<S[K]>) => S[K] {
    return (updaterOrValue: Updater<S[K]>) => {
        const current = get()[key];
        let next: S[K] =
            typeof updaterOrValue === 'function'
                ? (updaterOrValue as (prev: S[K]) => S[K])(current)
                : updaterOrValue;
        if (transform) next = transform(next);
        set({ [key]: next } as unknown as Partial<S>);
        return get()[key];
    };
}

export const useTaskTableStore = create<TaskTableStore>()(
    persist(
        (set, get) => ({
            maxRows: 100,
            setMaxRows: (newMaxRows: number) => {
                set({ maxRows: newMaxRows });
            },
            getMaxRows: () => get().maxRows,

            pagination: { pageIndex: 0, pageSize: 10 },
            setPagination: zustandSetter(get, set, 'pagination'),
            getPagination: () => get().pagination,

            filters: [] as ColumnFiltersState,
            setFilters: zustandSetter(
                get,
                set,
                'filters',
                (v: ColumnFiltersState) => v.map(dateRangeFilter)
            ),
            getFilters: () => get().filters,

            sorting: [] as SortingState,
            setSorting: zustandSetter(get, set, 'sorting'),
            getSorting: () => get().sorting,

            columnOrder: [] as ColumnOrderState,
            setColumnOrder: zustandSetter(get, set, 'columnOrder'),
            getColumnOrder: () => get().columnOrder,

            density: 'comfortable' as MRT_DensityState,
            setDensity: zustandSetter(get, set, 'density'),
            getDensity: () => get().density,

            columnVisibility: {
                task_num_attempts: false,
                task_max_attempts: false,
            } as VisibilityState,
            setColumnVisibility: zustandSetter(get, set, 'columnVisibility'),
            getColumnVisibility: () => get().columnVisibility,

            filterVisibility: true,
            setFilterVisibility: zustandSetter(get, set, 'filterVisibility'),
            getFilterVisibility: () => get().filterVisibility,
        }),
        {
            name: 'TaskTable',
            storage: taskTableStorage,
            partialize: state => ({
                maxRows: state.maxRows,
                filters: state.filters,
                sorting: state.sorting,
                columnOrder: state.columnOrder,
                pagination: state.pagination,
                density: state.density,
                columnVisibility: state.columnVisibility,
                filterVisibility: state.filterVisibility,
            }),
        }
    )
);
