<a name="marimba-development-environment-setup-guide-top"></a>
# Marimba Development Environment Setup Guide

This guide provides a detailed walkthrough for setting up a development environment for Marimba. Follow these instructions to contribute to Marimba or to execute the project locally for development purposes.

## Table of Contents

- [Clone the Repository](#clone-the-repository)
- [Project Structure](#project-structure)
- [Set up Poetry Environment](#set-up-poetry-environment)
- [Build Marimba](#build-marimba)

---

<a name="clone-the-repository"></a>
## Clone the Repository

Start by cloning the Marimba repository to your local machine. Open a terminal and execute the following command:

```bash
git clone https://bitbucket.csiro.au/scm/biaa/marimba.git
```

<p align="right">(<a href="#marimba-development-environment-setup-guide-top">back to top</a>)</p>

---

<a name="project-structure"></a>
## Project Structure

The architecture of the Marimba project is organised according to best practices outlined in the following resources:

* [The optimal python project structure](https://awaywithideas.com/the-optimal-python-project-structure/)
* [Structuring Your Project — The Hitchhiker's Guide to Python](https://docs.python-guide.org/writing/structure/)

The repository structure is as follows:

```plaintext
marimba
├── docs                        - Documentation files for Marimba
├── img                         - Images for this README.md file
├── marimba                     - Source directory containing the Marimba Python CLI application code
│   │
│   ├── commands                - Command definitions and CLI logic
│   ├── core                    - Core functionalities and data structures
│   ├── utils                   - Utility modules, helper functions, and common code
│   │
│   └── marimba.py              - Main Python application entry point
│
├── tests                       - Unit tests for the application
│
├── .flake8                     - Custom flake8 linting settings
├── .gitignore                  - Specifies files and folders to be ignored by Git
├── .isort.cfg                  - Configuration settings for isort tool
├── .pre-commit-config.yaml     - Configuration for pre-commit hooks that can be executed locally and in CI
├── LICENSE                     - License information for the project
├── pyproject.toml              - Custom Python Black code formatting settings
└── README.md                   - Project readme file providing an overview and setup instructions for the project
```

<p align="right">(<a href="#marimba-development-environment-setup-guide-top">back to top</a>)</p>

---

<a name="set-up-poetry-environment"></a>
## Set up Poetry Environment

The Python dependencies for Marimba are managed using [Poetry](https://python-poetry.org/). To install Poetry, execute:

```bash
pip install poetry
```

Navigate to the root directory of the Marimba project and run the following commands to set up the Poetry environment:

```bash
poetry install
poetry shell
```

This will create a new Python virtual environment, install the package dependencies, and activate the environment. You can confirm the successful activation by running:

```bash
marimba --help
```

<p align="right">(<a href="#marimba-development-environment-setup-guide-top">back to top</a>)</p>

---

<a name="build-marimba"></a>
## Build Marimba

To package the Marimba application as a Python wheel, execute:

```bash
poetry build
```

This command will generate a `dist` directory containing the built wheel package. This package can then be installed on other systems using pip:

```bash
pip install dist/marimba-0.1.0-py3-none-any.whl
```

<p align="right">(<a href="#marimba-development-environment-setup-guide-top">back to top</a>)</p>