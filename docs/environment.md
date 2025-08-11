<a name="marimba-development-environment-setup-guide-top"></a>
# Marimba Development Environment Setup Guide

This guide provides a detailed walkthrough for setting up a development environment for Marimba. Follow these instructions to contribute to Marimba or to execute the project locally for development purposes.

## Table of Contents

- [Clone the Repository](#clone-the-repository)
- [Project Structure](#project-structure)
- [Set up UV Environment](#set-up-uv-environment)
- [Code Quality Tools](#code-quality-tools)
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
marimba/
├── config/                     - Configuration files for tools (mypy, pytest, bandit, etc.)
├── dist/                       - Build distribution files (created during build)
├── docs/                       - Documentation files
│   ├── img/                    - Documentation images
│   └── templates/              - Template files for documentation
├── marimba/                    - Source code directory
│   ├── core/                   - Core functionality
│   │   ├── cli/                - Command line interface components
│   │   ├── distribution/       - Distribution target implementations
│   │   ├── parallel/           - Parallel processing functionality
│   │   ├── schemas/            - Data schemas for different formats
│   │   ├── utils/              - Utility modules and common functions
│   │   └── wrappers/           - Wrapper classes for core components
│   ├── lib/                    - Library modules for various functionalities
│   └── main.py                 - Main application entry point
├── tests/                      - Unit tests
│   └── core/                   - Tests for core modules
│       ├── cli/                - Tests for CLI modules
│       ├── distribution/       - Tests for distribution modules
│       ├── parallel/           - Tests for parallel modules
│       ├── schemas/            - Tests for schema modules
│       ├── utils/              - Tests for utility modules
│       └── wrappers/           - Tests for wrapper modules
├── .gitignore                  - Git ignore patterns
├── .pre-commit-config.yaml     - Pre-commit hook configuration
├── LICENSE                     - Project license file
├── pyproject.toml              - Project configuration and dependencies
└── README.md                   - Project overview and documentation
```

<p align="right">(<a href="#marimba-development-environment-setup-guide-top">back to top</a>)</p>

---

<a name="set-up-uv-environment"></a>
## Set up UV Environment

The Python dependencies for Marimba are managed using [UV](https://github.com/astral-sh/uv), a fast Python package installer and resolver. To set up your development environment:

```bash
# Install the package in development mode with dev dependencies
# This creates a virtual environment automatically and installs all dependencies
uv sync --group dev --python 3.12

# Activate the virtual environment (if not already activated) on Linux/Mac
source .venv/bin/activate
# or on Windows
.venv\Scripts\activate
```

This will create a new Python virtual environment, install the package dependencies, and activate the environment. You can confirm the successful activation by running:

```bash
marimba --help
```

<p align="right">(<a href="#marimba-development-environment-setup-guide-top">back to top</a>)</p>

---

<a name="code-quality-tools"></a>
## Code Quality Tools

Marimba uses several code quality tools that are configured in the pre-commit hooks:

### Pre-commit Hooks

The project includes pre-commit hooks to ensure code quality. Install them with:

```bash
pre-commit install
```

Our pre-commit configuration includes these hooks:

1. **Ruff** - For fast, comprehensive linting
   - Auto-fixes issues when possible
   - Uses configuration in the project files

2. **Black** - For code formatting
   - Line length: 120 characters
   - Ensures consistent code style

3. **Deptry** - For dependency validation
   - Ensures all imports are declared in dependencies
   - Prevents unused dependency accumulation

4. **Mypy** - For static type checking
   - Configuration: `config/mypy.ini`
   - Verifies type annotations

5. **Bandit** - For security linting
   - Configuration: `config/bandit.yml`
   - Identifies potential security issues

6. **Pytest** - For running tests
   - Configuration: `config/pytest.ini`
   - Ensures your changes don't break existing functionality

### Running Hooks Manually

You can run the pre-commit hooks manually to check your code:

```bash
# Run all hooks on all files
pre-commit run --all-files

# Run a specific hook
pre-commit run ruff --all-files
pre-commit run black --all-files
pre-commit run deptry --all-files
pre-commit run mypy --all-files
pre-commit run bandit --all-files
pre-commit run pytest --all-files
```

<p align="right">(<a href="#marimba-development-environment-setup-guide-top">back to top</a>)</p>

---

<a name="build-marimba"></a>
## Build Marimba

To package the Marimba application as a Python wheel:

```bash
# Using Hatch (via uv)
uv pip install build
python -m build
```

This command will generate a `dist` directory containing the built wheel package. This package can then be installed on other systems:

```bash
uv pip install dist/marimba-1.0.0-py3-none-any.whl
```

<p align="right">(<a href="#marimba-development-environment-setup-guide-top">back to top</a>)</p>