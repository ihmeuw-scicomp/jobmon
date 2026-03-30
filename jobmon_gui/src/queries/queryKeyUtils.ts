/**
 * Extract a numeric value from a query key at a given index.
 * Returns null if the index is out of bounds or the value is nullish.
 */
export function extractNumericParam(
    queryKey: readonly unknown[],
    index: number
): number | null {
    if (index >= queryKey.length) return null;
    const val = queryKey[index];
    if (val == null) return null;
    return typeof val === 'number' ? val : Number(val) || null;
}

/**
 * Build axios params with an optional workflow_run_id.
 * Returns undefined when wfrId is null (no param sent).
 */
export function wfrParams(
    wfrId: number | null,
    extra?: Record<string, string>
): Record<string, string> | undefined {
    if (wfrId == null && !extra) return undefined;
    const params: Record<string, string> = { ...extra };
    if (wfrId != null) {
        params.workflow_run_id = String(wfrId);
    }
    return Object.keys(params).length > 0 ? params : undefined;
}
