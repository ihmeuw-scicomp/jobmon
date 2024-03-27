# Jobmon

## Table of Contents

- [Introduction](#introduction)
- [Description](#description)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Requirements](#requirements)
- [Documentation](#documentation)
- [Branching Strategy](#branching-strategy)
- [Contributing](#contributing)
- [Changelog](#changelog)
- [License](#license)

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

## Branching Strategy

This project utilizes a branching strategy that emphasizes release branches and semantic versioning, facilitating orderly development, feature addition, bug fixes, and updates.

### Overview

- **Main Branch**: The `main` branch maintains the latest stable release of the project. It represents the culmination of all development efforts into a stable version ready for production use.
- **Release Branches**: These branches, named according to semantic versioning as `release/X.Y`, host development for upcoming minor or major releases. When starting a new major or minor version, it is branched off from the latest stable version in `main`.
- **Feature and Bug Fix Branches**: Development of new features and bug fixes happens in branches derived from the appropriate `release/X.Y` branches. Once development is complete, reviewed, and tested, these changes are merged back into their respective `release/X.Y` branch.

### Semantic Versioning

We adopt [semantic versioning](https://semver.org/) for organizing our releases:

- **Major Version (X)**: Incremented for significant changes or incompatible API modifications.
- **Minor Version (Y)**: Incremented for backward-compatible enhancements.
- **Patch Version (Z)**: Incremented for backward-compatible bug fixes.

### Development and Release Process

1. **Starting New Features and Fixes**: Branch off from the corresponding `release/X.Y` branch for developing new features or addressing bugs. Ensure your branch name clearly reflects the purpose of the changes.
2. **Applying Bug Fixes**: If a bug fix applies to a release branch that has diverged from `main`, it should first be applied to the most current release branch where relevant, before merging into the specific `release/X.Y` branch.
3. **Pull Request (PR)**: Submit a PR against the `release/X.Y` branch from which you branched out. The PR must summarize the changes and include any pertinent information for the reviewers.
4. **Creating Tags and Merging to Main**:
    - Upon completion of a release cycle, a version tag following the `X.Y.Z` format is created for the `release/X.Y` branch.
    - This tagged `release/X.Y` branch is then merged into `main`, signifying the release of a new version.

## Changelog

For a detailed history of changes and version updates, please refer to the [CHANGELOG.md](CHANGELOG.md) file within this repository.

## License

This project is licensed under the Jobmon Non-commercial License, developed at the Institute for Health Metrics and Evaluation (IHME), University of Washington. The license allows for redistribution and use in source and binary forms, with or without modification, under the conditions that:

- The software is used solely for non-commercial purposes. Commercial use, including indirect commercial use such as content on a website that accepts advertising money, is not permitted under this license. However, use by a for-profit company in its research is considered non-commercial use.
- All redistributions of source code must retain the copyright notice, this list of conditions, and the following disclaimer.
- Redistributions in binary form must reproduce the copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
- Neither the name of the University of Washington nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

For commercial use rights, contact the University of Washington, CoMotion, at license@uw.edu or call 206-543-3970, and inquire about this project.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

For a full copy of the license, see the [LICENSE](LICENSE) file in this repository.
