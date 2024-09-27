# Changelog

All notable changes to the Jobmon GUI will be documented in this file.

## [Unreleased]
### Added
### Changed
### Fixed
### Deprecated
### Removed

## [3.3.1] -TBD

### User-Facing
#### Fixed
- Fixed a bug where the status date in the Task table on the Workflow Details page was showing in UTC instead of PDT/PST.
- Fixed a bug where the runtime and memory histograms weren't showing any values.
- Fixed a bug where clustered errors would only show a max of 10 errors.
- Fixed a bug where "Invalid Date" showed in the workflow information modal on the landing page.
- Fixed a bug where concurrency limit would show as "false" for any value that wasn't "No Limit".
- Added syntax highlighting and new lines for error log messages.
- Fixed a bug where the database connection information wasn't templating on the `jobmon_at_ihme` page.

## [3.3.0] - 2024-09-10

### User-Facing
#### Added
- Added a `Clustered Errors` table to the `Workflow Details` page.
- Added a modal that shows users the details of their clustered errors.
- Workflow submitted date end time filter to `Workflow Overview` page.

#### Changed
- Removed table from `Workflow Overview` page; now show a list of progress bars with information buttons.
- Changed look of filters on `Workflow Overview` page.
- Changed the Task and TaskInstance tables to use the `react-material-table` component.
- Persist users' filters on `Workflow Overview` page.

#### Fixed
- Fixed bug where workflow elapsed time was wrong. It's now workflow status date - workflow submitted date.

#### Removed
- Removed `Errors` tab on `Workflow Details` page; information is now on `Errors and Tasks` tab.

### Developer
#### Changed
- Added zustand store to filters on `Workflow Overview` page.

## [1.14.0] - 2024-06-07
### User-Facing
#### Added
- Added MetricMind and ImageLauncher links to left-hand navigation bar.
- Added a new resources column to the TaskInstance table on the `Task Details` page that shows the requested and 
utilized resources.
- Users are now able to filter their workflows by both workflow attribute key and workflow attribute value on the 
`Workflow Overview` page.

#### Fixed
- Fixed bug in the breadcrumb navigation where the buttons would occasionally take users to the wrong page
- Fixed bug where it shows "No Workflows Found" even if no filters were passed.

#### Changed 
- Default to showing only Workflows that were submitted in the last two weeks on the `Workflow Overview` page.
- Switched to server side pagination for the errors table on the `Workflow Details` page.
- Toggle button on `Task Details` error table toggles for the latest TaskInstance errors associated with the latest 
WorkflowRun.
- Changed workflow information on `Workflow Details` page from a hover zoom to an information button that will show a 
modal when clicked.

### Developer
#### Changed
- Change file structure to include assets, components, configs, screens, styles, and utils folders.
- Switched to Vite for local development server.
- Changed import paths to be relative instead of absolute.
- Show Jobmon GUI version number on the bottom of left of GUI.

## [1.13.0] - 2023-10-11
### Changed
- Show workflow version on Workflow Details page hover zoom.
- Change the resource usage runtime from displaying in seconds to days hours minutes seconds.
- Show "No workflows found" when no workflow were found on Landing Page.
- Change the Jobmon Document link to ReadTheDocs on the help page. 
- Limit errors returned on Workflow details page to 2000 per a task template.

### Added
- Add workflow ID filter to the landing page.

## [1.12.4] - 2023-09-25
### Changed
- Update colors/logo to match IHME site redesign.

## [1.12.3] - 2023-09-20
### Fixed
- Redeploy with necessary .env variables to populate DB credentials.

## [1.12.1] - 2023-09-16
### Fixed
- Change main landing page SQL query to fix error in MySQL.

## [1.12.0] - 2023-09-15
### Added
- New "Jobmon at IHME" page that explains installation at IHME.
- Show elapsed time of a Workflow on the Workflow Details hover zoom.

### Changed
- Changed all hover zooms on the Task Details page to clickable pop-ups
- Default to showing 50 workflows on the landing page instead of 10.

## [1.11.1] - 2023-07-13
### Fixed
- 1.11.0 inadvertently upgraded our version of SQLAlchemy which broke two of the server routes due to how we handled SQLAlchemy column labels. This release fixes that.

## [1.11.0] - 2023-07-13
### Added
- Add Bifrost left hand navigation bar to the Jobmon GUI.

## [1.10.2] - 2023-06-21
### Added
- Add Workflow Attribute filter to landing page

## [1.10.1] - 2023-05-19
### Fixed
- Fixed a bug where the Error log pop up modal on the WorkflowDetails page would should the wrong message if you weren't on the first page of pagination.

## [1.10.0] - 2023-05-12
### Changed
- Prominently show workflow status on Workflow Details page (moved details pop up from "details" button to a pop up on the status icon)
- Changed the TaskDetails page to be more based on Task name than Task ID
  - Added a details pop up that now shows Task ID, command, and status date
- Have the landing page filters take up less screen real estate and condense landing page Workflow table
- Progress bar pop ups say "No Limit" instead of 2147483647 for max concurrency limit
- Have search filters persist on Landing Page when going back via "Home" button
- Display both task_instance_error_log description column and task_instance stderr_log column in the error pop up modal on the TaskDetails.

### Added
- Added clear button to landing page that clears filters, workflows, and URL.

### Fixed
- Fix error in sorting TaskTemplate progress bars on the WorkflowDetails page.

## [1.9.5] - 2023-04-17
### Fixed
- Fix server mismatch

## [1.9.4] - 2023-04-17
### Removed
- Removed unnecessary extra column (Error Log) in the TaskInstance table on the Task Details page.

## [1.9.3] - 2023-04-14
### Fixed
- Fix bug in sorting by task statuses in task table on Task Details page.

## [1.9.2] - 2023-04-07
### Fixed
- Fix bug in task status mapping.

## [1.9.1] - 2023-04-07
### Fixed
- Fix bug where done statuses are shown as pending.

## [1.9.0] - 2023-04-05
### Changed
- Deployment to improve readability and visual appeal.

## [1.8.0] - 2023-03-21
### Fixed
- Fixed a bug that required users to reload page twice when changing Workflow ID in URL on the "Workflow Details" page.

### Added
- Added pagination to the Errors table on the Errors tab.

### Changed
- Changed Errors time from GMT to PDT
- Change TaskInstance table on "Task Details" page to handle stdout and stderr changes
  - stderr and stdout columns show the last lines of the log
  - When the cell is clicked it will show users the file path and log message

## [1.7.0] - 2023-03-09
### Added
- Added a status filter to the landing page
- Added breadcrumb navigation

### Removed
- Removed workflows from landing page that were run on the "Dummy" cluster

### Fixed
- Fixed bug in error filtering

### Changed
- Redesign of "Errors" tab on the Workflow Details page
  - Show last line of error log in table
  - Add modal to show full error log message
  - Allow users to toggle between showing all error messages and showing only the error messages associated with the last task instance for each task.

## [1.6.0] - 2023-02-09
### Fixed
- Fix bug where filtered data gets erased on page load of the landing page.

### Added
- Create a new Task Details page that shows:
  - The Task finite state machine.
  - The upstream and downstream tasks of the current task.
  - A task instance table. 
  - How min, max, mean number of attempts for a Workflow by TaskTemplate on the Workflow Details page.

## [1.5.0] - 2023-01-04
### Added
- Add a dropdown that shows Workflow Details (e.g. name, tool, args) on the Workflow page.
- Add filtering to Task Table on Workflow page.
- Users can now filter by other values besides username on landing page.

### Fixed
- Fix back button logic so you don't have to cycle through all tab clicks.

### Changed
- Change visual style of error tab on Workflow page.

## [1.4.0] - 2022-11-16
### Added
- Users can now export CSVs of the Workflows table and the Tasks table.
- Add hover over state legend that explains the different Jobmon states.
- Add histograms of runtime and memory on resource usage tab.

## [1.3.1] - 2022-11-07
### Changed
- Optimize routes that were slow on large Workflows (>50k Tasks)
- Switch GUI to use global traefik.

### Fixed
- Fix bug where task template progress bars where showing wrong number of tasks in each state.

## [1.3.0] - 2022-10-31
### Added
- Added resource usage tab.
  -Shows mean, median, minimum and maximum values for memory and runtime of all tasks associated with specified TaskTemplate Version and Workflow.
- Added error logs tab.
  - Shows all error logs associated with specified TaskTemplate Version and Workflow.
- When hovering over the progress bars you can see what the concurrency limit is set at.

## [1.2.2] - 2022-09-30
### Added
- Added a new state to the progress bar - scheduled.

### Fixed
- Fix problem with ReactRouter setup.
- Fix Kubernetes credentials/secrets.
- Fix bug in PST conversion.

### Changed
- Index landing page table by WorkflowRun ID instead of Workflow ID.

## [1.2.1] - 2022-09-14
### Changed
- Only check progress of workflows that are not in DONE state.

## [1.2.0] - 2022-09-09
### Added
- Added ELK stack to the GUI
- Workflow Details page
  - Workflow progress bar.
  - A progress bar for each TaskTemplate in the Workflow.
  - A table with Task information associated with a given TaskTemplate. The table can be populated by using the provided search bar or clicking a TaskTemplate name in the TaskTemplate section.
  - Unique URLs on the Workflow Details page.
- Add workflow args and update status date columns to landing page table.

### Changed
- Change GUI to use typescript.

## [1.1.0] - 2022-07-25
### Changed
- Change table to use react-boostrap-table-next component.

## [1.0.0] - 2022-07-08
- Initial release of Jobmon GUI.
