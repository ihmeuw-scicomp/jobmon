import dayjs from 'dayjs';
import {
    WorkflowSearchSettings,
    FilterValue,
} from '@jobmon_gui/stores/workflow_settings';

function isFilterValue(value: string | FilterValue): value is FilterValue {
    return (
        typeof value === 'object' &&
        value !== null &&
        ('include' in value || 'exclude' in value)
    );
}

function addFilterToParams(
    params: URLSearchParams,
    key: string,
    value: string | FilterValue
): void {
    if (typeof value === 'string') {
        if (value) {
            params.set(key, value);
        }
    } else if (isFilterValue(value)) {
        if (value.include && value.include.length > 0) {
            params.set(key, value.include.join(','));
        }
        if (value.exclude && value.exclude.length > 0) {
            value.exclude.forEach(excluded => {
                params.append(`${key}!`, excluded);
            });
        }
    }
}

/**
 * Build URLSearchParams from settings. When {@link forAPI} is true,
 * adds one day to date_submitted_end so the backend range is inclusive.
 */
export function settingsToURLSearchParams(
    settings: WorkflowSearchSettings,
    forAPI = false
): URLSearchParams {
    const urlParams = new URLSearchParams();

    addFilterToParams(urlParams, 'user', settings.user);
    addFilterToParams(urlParams, 'tool', settings.tool);
    addFilterToParams(urlParams, 'status', settings.status);

    if (settings.wf_name) urlParams.set('wf_name', settings.wf_name);
    if (settings.wf_args) urlParams.set('wf_args', settings.wf_args);
    if (settings.wf_name_contains) urlParams.set('wf_name_contains', 'true');
    if (settings.wf_args_contains) urlParams.set('wf_args_contains', 'true');
    if (settings.wf_id) urlParams.set('wf_id', settings.wf_id);

    if (settings.date_submitted) {
        urlParams.set(
            'date_submitted',
            dayjs(settings.date_submitted).format('YYYY-MM-DD')
        );
    }
    if (settings.date_submitted_end) {
        const end = dayjs(settings.date_submitted_end);
        urlParams.set(
            'date_submitted_end',
            (forAPI ? end.add(1, 'day') : end).format('YYYY-MM-DD')
        );
    }

    if (settings.wf_attributes) {
        for (const attr of settings.wf_attributes) {
            if (attr.key || attr.value) {
                urlParams.append('wf_attr', `${attr.key}:${attr.value}`);
            }
        }
    }

    return urlParams;
}

export function settingsToSearchParamsString(
    settings: WorkflowSearchSettings
): string {
    return settingsToURLSearchParams(settings).toString();
}

export function parseUrlFilterParam(
    searchParams: URLSearchParams,
    key: string
): string | FilterValue {
    const includeParam = searchParams.get(key);
    const excludeParams = searchParams.getAll(`${key}!`);

    const include = includeParam
        ? includeParam.split(',').filter(v => v.trim() !== '')
        : [];
    const exclude = excludeParams
        .flatMap(param => param.split(','))
        .filter(v => v.trim() !== '');

    if (include.length === 0 && exclude.length === 0) {
        return '';
    }

    if (include.length === 1 && exclude.length === 0) {
        return include[0];
    }

    return {
        include: include.length > 0 ? include : undefined,
        exclude: exclude.length > 0 ? exclude : undefined,
    };
}

export function filterValueToDisplayString(
    value: string | FilterValue
): string {
    if (typeof value === 'string') {
        return value;
    }
    if (isFilterValue(value)) {
        if (value.include && value.include.length > 0) {
            return value.include.join(',');
        }
        return '';
    }
    return '';
}

/**
 * Extract query string from location, handling HashRouter where
 * query params may be in the hash fragment (e.g., #/?user!=svc_fhs)
 */
export function getSearchParamsFromLocation(
    locationSearch: string
): URLSearchParams {
    let searchString = locationSearch;
    if (!searchString && window.location.hash.includes('?')) {
        const hashParts = window.location.hash.split('?');
        if (hashParts.length > 1) {
            searchString = '?' + hashParts[1];
        }
    }
    return new URLSearchParams(searchString);
}
