# Contributing Guide

Thank you for your interest in contributing to Marimba! This guide will help you understand our contribution process and coding standards to ensure your contributions can be efficiently integrated into the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
  - [Fork the Repository](#fork-the-repository)
  - [Clone Your Fork](#clone-your-fork)
  - [Set Up the Development Environment](#set-up-the-development-environment)
- [Development Workflow](#development-workflow)
  - [Create a Feature Branch](#create-a-feature-branch)
  - [Make Your Changes](#make-your-changes)
  - [Commit Your Changes](#commit-your-changes)
  - [Push Changes to Your Fork](#push-changes-to-your-fork)
  - [Create a Pull Request](#create-a-pull-request)
- [Code Standards](#code-standards)
  - [Code Style](#code-style)
  - [Type Hints](#type-hints)
  - [Documentation](#documentation)
  - [Testing](#testing)
- [Pre-commit Hooks](#pre-commit-hooks)
  - [Available Hooks](#available-hooks)
  - [Running Pre-commit Hooks](#running-pre-commit-hooks)
- [Pull Request Guidelines](#pull-request-guidelines)
- [Issue Reporting](#issue-reporting)
- [License](#license)

## Code of Conduct

By participating in this project, you are expected to uphold our Code of Conduct: be respectful, considerate, and collaborative.

## Getting Started

### Fork the Repository

Start by forking the [Marimba repository](https://github.com/csiro-fair/marimba) on GitHub:

1. Visit https://github.com/csiro-fair/marimba
2. Click the "Fork" button in the top-right corner
3. Select your GitHub account as the destination for the fork

### Clone Your Fork

Once you have forked the repository, clone your fork to your local machine:

```bash
git clone https://github.com/YOUR-USERNAME/marimba.git
cd marimba
```

### Set Up the Development Environment

Marimba uses [UV](https://github.com/astral-sh/uv) for dependency management. Follow these steps to set up your development environment:

1. Install UV if you haven't already:
   ```bash
   pip install uv
   ```

2. Create and activate a virtual environment:
   ```bash
   # Create a virtual environment
   uv venv --python=3.10
   
   # Activate the virtual environment
   source .venv/bin/activate  # On Linux/Mac
   # or
   .venv\Scripts\activate     # On Windows
   ```

3. Install the project dependencies:
   ```bash
   uv pip install -e ".[dev]"
   ```

## Development Workflow

### Create a Feature Branch

Before making changes, create a new branch for your feature or bugfix:

```bash
git checkout -b feature/your-feature-name
```

Use a descriptive branch name that reflects the purpose of your changes.

### Make Your Changes

Now you can make changes to the codebase. Be sure to follow our [Code Standards](#code-standards).

### Commit Your Changes

When you're ready to commit your changes, stage and commit them:

```bash
git add .
git commit -m "Add a descriptive commit message"
```

Our pre-commit hooks will automatically run when you commit, ensuring your code meets our standards.

### Push Changes to Your Fork

Push your changes to your fork on GitHub:

```bash
git push origin feature/your-feature-name
```

### Create a Pull Request

Once your changes are pushed to your fork, you can create a pull request:

1. Go to the [original Marimba repository](https://github.com/csiro-fair/marimba)
2. Click "Pull Requests" and then "New Pull Request"
3. Click "compare across forks" and select your fork and branch
4. Click "Create Pull Request"
5. Provide a clear description of your changes and reference any related issues
6. Submit the pull request

## Code Standards

### Code Style

Marimba follows a strict code style to maintain consistency across the codebase:

- **Line Length**: Maximum line length is 120 characters
- **Python Version**: All code must be compatible with Python 3.10+
- **Formatting**: We use [Black](https://black.readthedocs.io/) for consistent code formatting
- **Linting**: We use [Ruff](https://github.com/charliermarsh/ruff) for linting with our custom configuration

### Type Hints

Marimba uses type hints extensively to improve code quality and development experience:

- All functions and methods should include type annotations
- Use Python 3.10+ type hint syntax (e.g., `X | Y` instead of `Union[X, Y]`)
- Function return types must be explicitly annotated
- Use built-in types like `dict`, `list` rather than imports from the `typing` module

Example:

```python
def process_data(data: list[dict[str, str]], limit: int | None = None) -> dict[str, int]:
    # Function implementation
    result: dict[str, int] = {}
    return result
```

### Documentation

Good documentation is essential:

- All modules, classes, methods, and functions should have docstrings following the [Google style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
- Include examples in docstrings where appropriate
- Update relevant documentation files when adding or changing features

### Testing

Ideally, all code should be covered by tests:

- Write unit tests for new functionality when possible
- Ensure existing tests pass with your changes
- Tests should be placed in the `tests/` directory
- Use [pytest](https://docs.pytest.org/) for writing and running tests

Note: With the rapid rise of automated testing capabilities powered by large language models, we will be looking to upgrade the codebase with more comprehensive testing as a separate project in the future. However, basic testing for critical components is still expected for new contributions.

## Pre-commit Hooks

Marimba uses pre-commit hooks to enforce code quality standards automatically. These hooks run when you commit changes, ensuring that your code meets our standards before it's committed.

### Available Hooks

We use the following pre-commit hooks:

1. **Ruff** - For fast, comprehensive linting
   - Configuration: `config/.ruff.toml`
   - Auto-fixes issues when possible
   - Excludes test and documentation files

2. **Black** - For code formatting
   - Line length: 120 characters
   - Ensures consistent code style

3. **Mypy** - For static type checking
   - Configuration: `config/mypy.ini`
   - Verifies type annotations
   - Helps catch type-related errors

4. **Bandit** - For security linting
   - Configuration: `config/bandit.yml`
   - Identifies potential security issues
   - Set to medium severity level

5. **Pytest** - For running tests
   - Configuration: `config/pytest.ini`
   - Ensures your changes don't break existing functionality

### Running Pre-commit Hooks

Pre-commit hooks run automatically when you commit changes. You can also run them manually:

```bash
# Run all hooks on all files
pre-commit run --all-files

# Run a specific hook
pre-commit run ruff --all-files
pre-commit run black --all-files
pre-commit run mypy --all-files
pre-commit run bandit --all-files
pre-commit run pytest --all-files
```

If a hook fails, fix the issues and try committing again.

#### Ruff Configuration

Marimba uses a custom Ruff configuration with rules carefully selected for our project. Key points:

- Targets Python 3.10+
- Uses a line length of 120 characters
- Follows the Google docstring convention
- Excludes certain directories (tests, docs, etc.)
- Ignores specific rules that don't align with our workflow (detailed in `config/.ruff.toml`)

#### Mypy Configuration

Our static type checking is configured to:

- Target Python 3.10+
- Use strict mode for thorough type checking
- Warn about returning `Any` types
- Ignore missing imports for third-party libraries

## Pull Request Guidelines

To ensure your pull request is accepted:

1. **Follow the code standards** outlined in this document
2. **Write or update tests** for the changes you make, ideally
3. **Update documentation** if you're changing functionality
4. **Reference issues** in your pull request description
5. **Keep pull requests focused** - address one concern per PR
6. **Be responsive to feedback** during the review process

## Issue Reporting

If you find a bug or want to request a feature:

1. Check existing issues to avoid duplicates
2. Use the issue templates when available
3. Provide clear, detailed information about the issue or feature
4. Include steps to reproduce bugs when possible
5. Be responsive to questions about your issue

## License

By contributing to Marimba, you agree that your contributions will be licensed under the project's [CSIRO BSD/MIT License](../LICENSE).

---

Thank you for contributing to Marimba! Your efforts help improve this tool for the scientific community.
