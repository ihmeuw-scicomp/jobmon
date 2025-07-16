# Task Concurrency Timeline Tab

## Overview

A new "Concurrency" tab has been added to the Workflow Details screen that displays a real-time timeline of concurrent task execution across different task templates.

## Features

### 📊 **Interactive Timeline Chart**
- Stacked area chart showing concurrent task counts over time
- Color-coded by task template for easy identification
- Responsive design that adapts to screen size

### ⏱️ **Multiple Time Ranges**
- 1 Hour (default)
- 6 Hours  
- 24 Hours
- 7 Days

### 🔄 **Real-time Updates**
- Automatic refresh every 30 seconds
- Manual refresh button
- Smart retry logic with exponential backoff

### 📁 **Data Export**
- Export chart data to CSV format
- Includes timestamp and concurrent counts for each task template

### 🎯 **Fallback Handling**
- Shows demo data when API is unavailable
- Clear error messages with retry options
- Graceful degradation

## Implementation Details

### Files Created/Modified

1. **`TaskConcurrencyTimeline.tsx`** - New component
   - Production-ready React component using Plotly.js
   - Integrates with existing Material-UI design system
   - Uses React Query for data fetching and caching

2. **`WorkflowDetails.tsx`** - Modified
   - Added new "Concurrency" tab (index 2)
   - Integrated TaskConcurrencyTimeline component

3. **`ApiUrls.ts`** - Modified  
   - Added `get_task_concurrency_url()` function

### Dependencies

The component requires these packages (already in use):
- `react-plotly.js` - For interactive charts
- `plotly.js` - Chart rendering engine  
- `dayjs` - Date/time formatting
- `@tanstack/react-query` - Data fetching
- `@mui/material` - UI components

### API Integration

**Endpoint**: `GET /workflow/{workflow_id}/task_concurrency`

**Query Parameters**:
- `time_range`: '1h' | '6h' | '24h' | '7d'
- `granularity`: 'minute' (fixed for now)

**Expected Response**:
```typescript
{
  data?: [
    {
      timestamp: string,
      task_template_name: string, 
      concurrent_count: number
    }
  ]
}
```

### Demo Mode

When the API endpoint is not available, the component automatically falls back to generating realistic demo data that:
- Follows business hour patterns (higher load 9-5)
- Shows correlation between task templates
- Provides visual feedback that it's demo data

## Usage

The tab appears automatically in the Workflow Details screen alongside "Task Templates" and "DAG Viz" tabs. No additional configuration is required.

### User Interactions

1. **Time Range Selection**: Dropdown to change the time window
2. **Export**: Button to download data as CSV
3. **Refresh**: Manual refresh button for immediate updates
4. **Chart Interactions**: Zoom, pan, hover tooltips (via Plotly)

## Future Enhancements

Potential improvements that could be added:

1. **Filtering**: Filter by specific task templates
2. **Zoom Controls**: Preset zoom levels and date range picker
3. **Annotations**: Mark significant events on the timeline
4. **Alerts**: Threshold-based notifications for high concurrency
5. **Comparison**: Side-by-side comparison of different time periods
6. **Granularity**: User-selectable time granularity (minute/hour/day)

## Performance Considerations

- Chart is limited to reasonable data points for performance
- Uses React Query caching to minimize API calls
- Memoized data processing to prevent unnecessary re-renders
- Responsive design handles various screen sizes efficiently

## Error Handling

- Network errors: Shows retry button with exponential backoff
- No data: Clear message with fallback to demo data
- Loading states: Spinner with descriptive text
- Export errors: Console logging with user feedback 