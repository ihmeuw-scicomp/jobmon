import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import advancedFormat from "dayjs/plugin/advancedFormat";
import timezone from "dayjs/plugin/timezone";
import {useDisplayTimeFormatStore, useDisplayTimezoneStore} from "@jobmon_gui/stores/DateTime.ts";


export const formatDayjsDate = (date: dayjs.Dayjs)=> {
    dayjs.extend(utc)
    dayjs.extend(advancedFormat);
    dayjs.extend(timezone);
    return date.tz(useDisplayTimezoneStore.getState().timezone || Intl.DateTimeFormat().resolvedOptions().timeZone).format(useDisplayTimeFormatStore.getState().timeFormat)
}

export const formatJobmonDate = (date: string|null|undefined)=> {
    if(!date)
        return ""
    dayjs.extend(utc)
    return formatDayjsDate(dayjs.utc(date))
}