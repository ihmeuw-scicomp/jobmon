# Jobmon

## Table of Contents

- [Introduction](#introduction)
- [Description](#description)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Requirements](#requirements)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [Changelog](#changelog)
- [License](#license)
- [Authors and Acknowledgment](#authors-and-acknowledgment)
- [Security](#security)

## Introduction

Jobmon is a Python package developed by IHME's Scientific Computing team, designed to simplify and standardize the process of job monitoring and workflow management in computational projects. It facilitates the tracking of job statuses, manages dependencies, and streamlines the execution of complex workflows across various computing environments.

## Description

The tool aims to enhance productivity and ensure computational tasks are efficiently managed and executed, offering a robust solution for handling large-scale, data-driven analyses in research and development projects.

## Features

- **Workflow Management**: Easily define and manage workflows with multiple interdependent tasks.
- **Status Tracking**: Real-time tracking of job statuses to monitor the progress of computational tasks.
- **Error Handling**: Automatically detect and report errors in jobs, supporting swift resolution and rerun capabilities.
- **Compatibility**: Designed to work seamlessly across different computing environments, including HPC clusters and cloud platforms.

## Installation

To install Jobmon, use the following pip command:

```bash
pip install jobmon_client[server]
```

## Usage

Refer to the [quickstart](https://jobmon.readthedocs.io/en/latest/quickstart.html#create-a-workflow) to get started with a sample workflow

## Requirements

- Python 3.8+

## Documentation

For comprehensive documentation, visit [readthedocs](https://jobmon.readthedocs.io/en/latest/#).

## Contributing

We encourage contributions from the community. If you're interested in improving JobMon or adding new features, please refer to our [developer guide](https://jobmon.readthedocs.io/en/latest/developers_guide/developer-start.html) for python client contributions or the GUI [README.md](jobmon_gui/README.md) for visualization contributions.

## Changelog

For a detailed history of changes and version updates, please refer to the [CHANGELOG.md](CHANGELOG.md) file within this repository.

## License

Jobmon is licensed under ??? - see the LICENSE file for details.

## Authors and Acknowledgment

Thanks to the IHME's Scientific Computing team and all contributors to the Jobmon project.

## Security

Report security vulnerabilities by creating an issue in the GitHub repository.
