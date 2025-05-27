import { create } from 'zustand';
import { persist } from 'zustand/middleware';

export type DisplayTimezoneStore = {
  timezone: string;
  set: (newTimezone: string) => void;
  get: () => string;
  clear: () => void;
  getSupportedTimezones: () => string[];
  getBrowserDefaultTimezone: () => string;
};

export const useDisplayTimezoneStore = create<DisplayTimezoneStore>()(
  persist(
    (set, get) => ({
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      set: (newTimezone: string) => set(() => ({ timezone: newTimezone })),
      get: () => get().timezone,
      clear: () => {
        set(() => ({
          timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
        }));
      },
      getSupportedTimezones: () => Intl.supportedValuesOf('timeZone'),
      getBrowserDefaultTimezone: () =>
        Intl.DateTimeFormat().resolvedOptions().timeZone,
    }),
    {
      name: 'DisplayTimeZone',
    }
  )
);
export type DisplayTimeFormatStore = {
  timeFormat: string;
  set: (newTimeFormat: string) => void;
  get: () => string;
  clear: () => void;
  getTimeFormatList: () => string[];
  getDefault: () => string;
};
export const defaultTimeFormat = 'YYYY-MM-DD HH:mm:ss z';

export const useDisplayTimeFormatStore = create<DisplayTimeFormatStore>()(
  persist(
    (set, get) => ({
      timeFormat: defaultTimeFormat,
      set: (newTimeFormat: string) =>
        set(() => ({ timeFormat: newTimeFormat })),
      get: () => get().timeFormat,
      clear: () => {
        set(() => ({ timeFormat: defaultTimeFormat }));
      },
      getTimeFormatList: () => [
        defaultTimeFormat,
        'dddd, MMMM DD, YYYY [at] hh:mm:ss A z', // ex: Thursday, November 21, 2024 at 2:59:43 PM PST
        'MM/DD/YYYY HH:mm:ss z',
      ],
      getDefault: () => defaultTimeFormat,
    }),
    {
      name: 'DisplayTimeFormat',
    }
  )
);
