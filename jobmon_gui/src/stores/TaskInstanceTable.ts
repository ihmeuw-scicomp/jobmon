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

export const job_info_table_time_column_names = [];

const taskTableStorage = createJSONStorage(() => localStorage, {
  reviver: (key, value) => {
    // Custom storage retriever to convert date strings back into dayjs objects
    // We are looking for:
    //   1. A key named "value"
    //   2. An array with a length of 2
    //   3. Either value[0] or value[1] to be a string that can be parsed with dayjs
    if (key == 'pageIndex') {
      // If we don't store pageIndex with the rest of local storage the page behaves strangely,
      // but we want to default to page zero on page load, so overwrite the value here
      return 0;
    }
    if (
      key == 'value' &&
      Array.isArray(value) &&
      value.length == 2 &&
      (dayjs(value[0]).isValid() || dayjs(value[1]).isValid())
    ) {
      const range_start = value[0] ? dayjs(value[0]) : '';
      const range_end = value[1] ? dayjs(value[1]) : '';
      const new_value = [range_start, range_end];
      return new_value;
    }

    return value;
  },
});

export interface ColumnFilter {
  id: string;
  value: string | number | Dayjs[] | unknown;
}

export type ColumnFiltersState = ColumnFilter[];

export type TaskInstanceTableStore = {
  maxRows: number;
  setMaxRows: (newMaxRows: number) => void;
  getMaxRows: () => number;

  pagination: PaginationState;
  getPagination: () => PaginationState;
  setPagination: React.Dispatch<React.SetStateAction<PaginationState>>;

  filters: ColumnFiltersState;
  getFilters: () => ColumnFiltersState;
  setFilters: React.Dispatch<React.SetStateAction<ColumnFiltersState>>;
  // clearFilters: () => void

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
  // Detect the date/time fields, and ensure that the first value is less than
  // or equal to the second value
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

export const useTaskInstanceTableStore = create<TaskInstanceTableStore>()(
  persist(
    (set, get) => ({
      maxRows: 100,
      setMaxRows: (newMaxRows: number) => {
        set({ ...get(), maxRows: newMaxRows });
      },
      getMaxRows: () => {
        return get().maxRows;
      },

      pagination: {
        pageIndex: 0,
        pageSize: 10, //customize the default page size
      },
      setPagination: (updaterOrValue: Updater<PaginationState>) => {
        // updateOrValue will be of one of these types: ((prevState: S) => S) | S

        if (typeof updaterOrValue === 'function') {
          // the input variable is a function
          set(() => ({
            ...get(),
            pagination: updaterOrValue(get().pagination),
          }));
        } else {
          // the input variable is a value
          set(() => ({ ...get(), pagination: updaterOrValue }));
        }
        return get().pagination;
      },
      getPagination: () => {
        return get().pagination;
      },
      filters: [],
      setFilters: (updaterOrValue: Updater<ColumnFiltersState>) => {
        // updateOrValue will be of one of these types: ((prevState: S) => S) | S

        if (typeof updaterOrValue === 'function') {
          // the input variable is a function
          set(() => ({
            ...get(),
            filters: updaterOrValue(get().filters).map(dateRangeFilter),
          }));
        } else {
          // the input variable is a value
          set(() => ({
            ...get(),
            filters: updaterOrValue.map(dateRangeFilter),
          }));
        }
        return get().filters;
      },
      getFilters: () => {
        return get().filters;
      },
      sorting: [],
      setSorting: (updaterOrValue: Updater<SortingState>) => {
        // updateOrValue will be of one of these types: ((prevState: S) => S) | S

        if (typeof updaterOrValue === 'function') {
          // the input variable is a function
          set(() => ({ ...get(), sorting: updaterOrValue(get().sorting) }));
        } else {
          // the input variable is a value
          set(() => ({ ...get(), sorting: updaterOrValue }));
        }
        return get().sorting;
      },
      getSorting: () => {
        return get().sorting;
      },
      columnOrder: [],
      setColumnOrder: (updaterOrValue: Updater<ColumnOrderState>) => {
        // updateOrValue will be of one of these types: ((prevState: S) => S) | S

        if (typeof updaterOrValue === 'function') {
          // the input variable is a function
          set(() => ({
            ...get(),
            columnOrder: updaterOrValue(get().columnOrder),
          }));
        } else {
          // the input variable is a value
          set(() => ({ ...get(), columnOrder: updaterOrValue }));
        }
        return get().columnOrder;
      },
      getColumnOrder: () => {
        return get().columnOrder;
      },
      density: 'comfortable',
      setDensity: (updaterOrValue: Updater<MRT_DensityState>) => {
        // updateOrValue will be of one of these types: ((prevState: S) => S) | S

        if (typeof updaterOrValue === 'function') {
          // the input variable is a function
          set(() => ({ ...get(), density: updaterOrValue(get().density) }));
        } else {
          // the input variable is a value
          set(() => ({ ...get(), density: updaterOrValue }));
        }
        return get().density;
      },
      getDensity: () => {
        return get().density;
      },
      columnVisibility: { elapsed: false, time_limit: false },
      setColumnVisibility: (updaterOrValue: Updater<VisibilityState>) => {
        // updateOrValue will be of one of these types: ((prevState: S) => S) | S

        if (typeof updaterOrValue === 'function') {
          // the input variable is a function
          set(() => ({
            ...get(),
            columnVisibility: updaterOrValue(get().columnVisibility),
          }));
        } else {
          // the input variable is a value
          set(() => ({ ...get(), columnVisibility: updaterOrValue }));
        }
        return get().columnVisibility;
      },
      getColumnVisibility: () => {
        return get().columnVisibility;
      },
      filterVisibility: true,
      setFilterVisibility: (updaterOrValue: Updater<boolean>) => {
        // updateOrValue will be of one of these types: ((prevState: S) => S) | S

        if (typeof updaterOrValue === 'function') {
          // the input variable is a function
          set(() => ({
            ...get(),
            filterVisibility: updaterOrValue(get().filterVisibility),
          }));
        } else {
          // the input variable is a value
          set(() => ({ ...get(), filterVisibility: updaterOrValue }));
        }
        return get().filterVisibility;
      },
      getFilterVisibility: () => {
        return get().filterVisibility;
      },
    }),
    {
      name: 'TaskInstanceTable',
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
