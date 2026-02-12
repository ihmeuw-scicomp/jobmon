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
    params: Record<string, string>,
    key: string,
    value: string | FilterValue,
): void {
    if (typeof value === 'string') {
        if (value) {
            params[key] = value;
        }
    } else if (isFilterValue(value)) {
        if (value.include && value.include.length > 0) {
            params[key] = value.include.join(',');
        }
        if (value.exclude && value.exclude.length > 0) {
            value.exclude.forEach(excluded => {
                params[`${key}!`] = excluded;
            });
        }
    }
}

export function settingsToSearchParamsString(
    settings: WorkflowSearchSettings
): string {
    const params: Record<string, string> = {};

    addFilterToParams(params, 'user', settings.user);
    addFilterToParams(params, 'tool', settings.tool);
    addFilterToParams(params, 'status', settings.status);

    if (settings.wf_name) params.wf_name = settings.wf_name;
    if (settings.wf_args) params.wf_args = settings.wf_args;
    if (settings.wf_attribute_key)
        params.wf_attribute_key = settings.wf_attribute_key;
    if (settings.wf_attribute_value)
        params.wf_attribute_value = settings.wf_attribute_value;
    if (settings.wf_id) params.wf_id = settings.wf_id;

    const date_submitted = dayjs(settings.date_submitted).format('YYYY-MM-DD');
    const date_submitted_end = dayjs(settings.date_submitted_end).format(
        'YYYY-MM-DD'
    );
    if (date_submitted) params.date_submitted = date_submitted;
    if (date_submitted_end) params.date_submitted_end = date_submitted_end;

    return new URLSearchParams(params).toString();
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

