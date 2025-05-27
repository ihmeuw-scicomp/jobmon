import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import advancedFormat from 'dayjs/plugin/advancedFormat';
import timezone from 'dayjs/plugin/timezone';
import {
    useDisplayTimeFormatStore,
    useDisplayTimezoneStore,
} from '@jobmon_gui/stores/DateTime.ts';

dayjs.extend(utc);
dayjs.extend(advancedFormat);
dayjs.extend(timezone);

export const formatDayjsDate = (date: dayjs.Dayjs) => {
    return date
        .tz(
            useDisplayTimezoneStore.getState().timezone ||
                Intl.DateTimeFormat().resolvedOptions().timeZone
        )
        .format(useDisplayTimeFormatStore.getState().timeFormat);
};

export const formatJobmonDate = (
    date: string | dayjs.Dayjs | null | undefined
) => {
    if (!date) return '';

    const parsedDate =
        typeof date === 'string' ? dayjs.tz(date, 'America/Los_Angeles') : date;

    return formatDayjsDate(parsedDate);
};
