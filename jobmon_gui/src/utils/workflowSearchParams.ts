import dayjs from 'dayjs';
import { WorkflowSearchSettings } from '@jobmon_gui/stores/workflow_settings';

export function settingsToSearchParamsString(
    settings: WorkflowSearchSettings
): string {
    const date_submitted = dayjs(settings.date_submitted).format('YYYY-MM-DD');
    const date_submitted_end = dayjs(settings.date_submitted_end).format(
        'YYYY-MM-DD'
    );

    const rawParams: Record<string, string> = {
        user: settings.user,
        tool: settings.tool,
        wf_name: settings.wf_name,
        wf_args: settings.wf_args,
        wf_attribute_key: settings.wf_attribute_key,
        wf_attribute_value: settings.wf_attribute_value,
        wf_id: settings.wf_id,
        date_submitted,
        date_submitted_end,
        status: settings.status,
    };

    const filteredParams = Object.fromEntries(
        Object.entries(rawParams).filter(
            ([_, value]) => value != null && value !== ''
        )
    );

    return new URLSearchParams(filteredParams).toString();
}

