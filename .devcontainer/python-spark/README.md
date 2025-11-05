# Python DevContainer - Complete Code Quality Suite

A development environment with 20+ integrated code quality tools, automated CI/CD validation, and zero-configuration setup. This DevContainer provides everything you need for professional Python development: formatters, linters, type checkers, security scanners, dependency analyzers, test runners, and documentation generators.

## 🚀 Why Use This DevContainer?

### Zero-Configuration Professional Development Environment
DevContainers provide a fully configured and isolated development environment inside a container, ensuring consistent, reproducible, and platform-independent setup across your entire team. No more "works on my machine" issues or lengthy environment setup procedures.

### Comprehensive Tool Integration
This DevContainer includes 20+ carefully curated industry-standard Python development tools:

- **🎨 Code Formatters**: Ruff - Ensure consistent code style across your team
- **🔍 Linters & Analyzers**: Pylint, Flake8, Ruff Linter - Catch bugs and enforce best practices
- **🔒 Type Checkers**: Mypy, Pyright, Pylance - Prevent type-related errors before workflow
- **🛡️ Security Scanners**: Bandit, Semgrep - Identify vulnerabilities and security issues
- **🔐 Secret Detection**: TruffleHog - Prevent credential leaks in your repository
- **🧪 Testing & Coverage**: Pytest with Coverage.py - Ensure code reliability and test completeness
- **📚 Documentation**: Sphinx - Generate professional documentation automatically

### Two Modes of Operation

#### Primary: Real-time Code Analysis (Opened Files)
Extensions provide instant feedback as you code, showing issues directly in the editor and Problems tab for the currently open file.

!["Preview of live problems tab showing issues"](./docs/preview_live_problems_tab.gif)

Code Coverage can be seen directly in the editor after running tests. The `if` line shows orange for partially covered by unit tests and the `print` line shows red for not covered.

![Preview Code Coverage in Editor](./docs/preview_coverage_in_editor.gif)


#### Secondary: Complete Repository Validation (All Files)
VS Code tasks run comprehensive analysis across your entire repository, simulating CI/CD pipeline checks locally.

!["Run all Code Quality tools as VSCode Tasks"](./docs/run_all_vscode_tasks.gif)

### Quick Setup

**Launch**: Open project in VS Code → `F1` → "Dev Containers: Reopen in Container"

### Architecture Benefits

This implementation uses Docker Compose for maximum flexibility:
- **Main Container**: Development environment with all tools pre-installed
- **Extensible**: Easy to add supporting services (databases, message queues, etc.)
- **Volume Mounts**: Persistent storage for workspace and configuration
- **Package Caching**: Docker volumes cache pip and Poetry packages for fast rebuilds.
- **Platform Independent**: Consistent experience across Windows, macOS, and Linux

## 🏁 Getting Started

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running
- [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) for VS Code

### Quick Installation

1. **📥 Clone This Example Repository**: 
   ```bash
   git clone https://github.com/KrijnvanderBurg/DevOps-Toolkit
   cd DevOps-Toolkit
   ```

2. **📦 Initialize Submodules** (Required):
   ```bash
   git submodule update --init --recursive
   ```
   
   **Why `--recursive`?** The `.devcontainer/.dotfiles` directory is a git submodule from another repository. This ensures all configuration files and scripts are properly downloaded. If you create a submodule of this devcontainer then its a nested submodule. 

3. **🐳 Launch Container**: Open project in VS Code, press `F1` → "Dev Containers: Reopen in Container"

4. **⚡ Verify Setup**: `Ctrl+Shift+P` → "Tasks: Run Task" → "test" (all tools should pass on a fresh project)

### Architecture Notes

- **Nested Repository Structure**: The `.devcontainer/.dotfiles` is a git submodule containing all tool configurations and scripts as a self-contained unit
- **Why Submodule**: This approach keeps the DevContainer configuration portable and reusable across multiple projects while maintaining version control over the development environment setup

> **🚀 Ready for production deployment?** This DevContainer uses identical configurations as the [Azure DevOps CI/CD templates](https://github.com/KrijnvanderBurg/.azuredevops). Your local quality checks **mirror your pipeline exactly** — when tests pass locally, they're guaranteed to pass in production.

**Pro Tips:**
- Run `test` task before committing to catch issues early
- Use individual tool tasks for focused debugging  
- Check the Problems panel for real-time feedback on open files
- Generated reports (`coverage.xml`, `JUNIT-TEST.xml`) provide detailed analysis

## � Observability & Tracing

This DevContainer includes pre-configured **Grafana** and **Jaeger** for distributed tracing and observability.

### Services

- **Grafana** <sup>[Docs](https://grafana.com/docs/) | [Github](https://github.com/grafana/grafana)</sup> - Open-source analytics and monitoring platform. Access at [http://localhost:3000](http://localhost:3000) (no login required)

- **Jaeger** <sup>[Docs](https://www.jaegertracing.io/docs/) | [Github](https://github.com/jaegertracing/jaeger)</sup> - Distributed tracing system for monitoring and troubleshooting microservices. Access UI at [http://localhost:16686](http://localhost:16686)

### Features

- **Automatic Configuration**: Grafana is pre-configured with Jaeger datasource
- **Custom Dashboard**: "Samara Tracing Dashboard" shows traces and service graphs
- **Real-time Updates**: Dashboard auto-refreshes every 5 seconds
- **Zero Setup**: Samara's telemetry automatically sends traces to Jaeger via OTLP

📖 **[Full Observability Documentation](./grafana/README.md)**

## �🛠️ List of Code Quality Tools

### Formatters

- **Ruff Formatter** <sup>[Docs](https://docs.astral.sh/ruff/) | [Github](https://github.com/astral-sh/ruff) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)</sup> - Lightning-fast Python formatter written in Rust, combining the functionality of Black, isort, and more. **Primary recommendation** for modern Python development.

### Linters & Code Quality

- **Ruff Linter** <sup>[Docs](https://docs.astral.sh/ruff/) | [Github](https://github.com/astral-sh/ruff) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)</sup> - Extremely fast linter with 800+ rules, replacing Flake8, pylint, and more. **Primary recommendation** for comprehensive code quality checks.

- **Pylint** <sup>[Docs](https://pylint.org/) | [Github](https://github.com/pylint-dev/pylint) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=ms-python.pylint)</sup> - Comprehensive static code analyzer that checks for errors, enforces coding standards, and suggests refactorings.

- **Flake8** <sup>[Docs](https://flake8.pycqa.org/) | [Github](https://github.com/PyCQA/flake8) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=ms-python.flake8)</sup> - Combines pycodestyle, pyflakes, and mccabe to check code style and quality.


### Type Checkers

- **Pylance** <sup>[Github](https://github.com/microsoft/pylance-release) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance)</sup> - Microsoft's fast, feature-rich Python language server powered by Pyright. Provides IntelliSense, type checking, and code navigation.

- **Mypy** <sup>[Docs](https://mypy-lang.org/) | [Github](https://github.com/python/mypy) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=ms-python.mypy-type-checker)</sup> - Optional static type checker that combines dynamic and static typing benefits.

- **Pyright** <sup>[Docs](https://microsoft.github.io/pyright) | [Github](https://github.com/microsoft/pyright) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=ms-pyright.pyright)</sup> - Fast, standards-based static type checker designed for large Python codebases.

### Security & Vulnerability Scanners

- **Bandit** <sup>[Docs](https://bandit.readthedocs.io) | [Github](https://github.com/PyCQA/bandit) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=nwgh.bandit)</sup> - Scans Python code for common security issues by analyzing AST patterns.


- **DevSkim** <sup>[Docs](https://github.com/microsoft/DevSkim/wiki) | [Github](https://github.com/microsoft/DevSkim) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=MS-CST-E.vscode-devskim)</sup> - Microsoft's security analysis framework for detecting vulnerabilities as you code.

### Secrets & Credentials Detection

- **TruffleHog** <sup>[Github](https://github.com/trufflesecurity/trufflehog)</sup> - Searches through git repositories for high-entropy strings and secrets, detecting passwords, API keys, and tokens.

### Dependency Analysis

- **Semgrep** <sup>[Docs](https://semgrep.dev/p/python) | [Github](https://github.com/semgrep/semgrep)</sup> - Fast static analysis tool for finding bugs, security vulnerabilities, and enforcing code standards.

### Testing

- **Pytest** <sup>[Docs](https://docs.pytest.org) | [Github](https://github.com/pytest-dev/pytest) | [Pypi](https://pypi.org/project/pytest/)</sup> - The pytest framework makes it easy to write small tests, yet scales to support complex functional testing for applications and libraries. [Config file](../.dotfiles/python/pytest.ini) | [AzDO pipeline](https://github.com/KrijnvanderBurg/.azuredevops/tree/main/v1/templates/atomic/python/test_pytest.yaml) | [DevContainer config](#pytest)

- **Code Coverage** <sup>[coverage.py](https://coverage.readthedocs.io/)</sup>: Checks code coverage of Python tests with coverage.py. [Config file](../.dotfiles/python/.coveragerc) | [AzDO pipeline](https://github.com/KrijnvanderBurg/.azuredevops/tree/main/v1/templates/atomic/python/pytest.yaml) | [DevContainer config](#pytest)


### Dependency management

- **Poetry** <sup>[Docs](https://python-poetry.org/) | [Github](https://github.com/python-poetry/poetry) | [Pypi]()</sup> - Poetry helps you declare, manage and install dependencies of Python projects, ensuring you have the right stack everywhere. [AzDO pipeline](https://github.com/KrijnvanderBurg/.azuredevops/tree/main/v1/templates/atomic/python/poetry-install-pyproject.yaml)

### Documentation

- **Sphinx** <sup>[Docs](https://www.sphinx-doc.org/) | [Github](https://github.com/sphinx-doc/sphinx) | [Pypi](https://pypi.org/project/Sphinx/)</sup> - Sphinx makes it easy to create intelligent and beautiful documentation. [Config file](hello_world/docs/) | [AzDO pipeline](https://github.com/KrijnvanderBurg/.azuredevops/tree/main/v1/templates/atomic/python/sphinx.yaml)

## ⚙️ VS Code Extensions Configuration

This DevContainer automatically installs and configures all necessary VS Code extensions. This section provides technical details about extension settings for developers who want to understand or customize the configuration.

### 🧩 Why Use VS Code Extensions?
Extensions improve your development workflow by providing real-time feedback, automated formatting, security scanning, and advanced code navigation—all directly in your editor. Properly configuring these extensions ensures that every team member benefits from consistent tooling, immediate issue detection, and seamless integration with your DevContainer setup.

!["Preview of live problems tab showing issues"](./docs/preview_live_problems_tab.gif)

#### 🎨 Formatting Extensions

##### Ruff (Primary Formatter)
Modern, fast formatter combining Black, isort, and more. Automatically runs on file save.

```json
"extensions": [
    "charliermarsh.ruff"
],
"settings": {
    "[python]": {
        "editor.formatOnSave": true,
        "editor.codeActionsOnSave": {
            "source.fixAll": "explicit",
            "source.organizeImports": "explicit"
        },
        "editor.defaultFormatter": "charliermarsh.ruff"
    },
    "ruff.lint.args": [
        "--config=${workspaceFolder}/path/to/.ruff.toml"
    ],
    "ruff.format.args": [
        "--config=${workspaceFolder}/path/to/.ruff.toml"
    ],
},
```

#### 🔍 Linting Extensions

##### Pylint
Comprehensive code analyzer. Shows issues in editor and Problems tab.

```json
"extensions": [
    "ms-python.pylint"
],
"settings": {

    "pylint.args": [
        "--rcfile",
        "${workspaceFolder}/path/to/.pylintrc"
    ],
    "pylint.cwd": "${workspaceFolder}",
    "pylint.enabled": true,
    "pylint.showNotification": "onError",
    "pylint.lintOnChange": true,
},
```

##### Flake8  
Style and quality checker. Shows issues in editor and Problems tab.

```json
"extensions": [
    "ms-python.flake8"
],
"settings": {
    "flake8.args": [
        "--config",
        "${workspaceFolder}/path/to/.flake8" 
    ],
    "flake8.cwd": "${workspaceFolder}",
    "flake8.showNotification": "onError",
    "flake8.enabled": true,
},
```

#### 🛡️ Security Extensions

##### Bandit
Security vulnerability scanner. Shows issues in editor and Problems tab.

```json
"extensions": [
    "nwghbandit.ini"
],
"settings": {
    "bandit.args": [
        "-c",
        "${workspaceFolder}/path/to/bandit.ini"
    ],
    "bandit.cwd": "${workspaceFolder}",
    "bandit.logLevel": "warning",
    "bandit.enabled": true,
    "bandit.showNotification": "onError"
},
```

#### 🔧 Type Checking Extensions

##### Mypy
Static type checker. Shows type issues in editor and Problems tab.

```json
"extensions": [
    "ms-python.flake8"
],
"settings": {
    "flake8.args": [
        "--config",
        "${workspaceFolder}/path/to/.flake8"
    ],
    "flake8.cwd": "${workspaceFolder}",
    "flake8.showNotification": "onError",
    "flake8.enabled": true,
    // ...
},
```

#### 🧪 Testing Extensions

##### Pytest
Test discovery and execution. Access via Testing panel in VS Code sidebar.

```json
"extensions": [],
"settings": {
    "python.testing.unittestEnabled": false,
    "python.testing.pytestEnabled": true,
    "python.testing.pytestArgs": [
        "-c",
        "${workspaceFolder}/path/to/pytest.ini"
    ],
    "python.languageServer": "Default",
```

##### Pylance (IntelliSense)
Microsoft's Python language server. Provides IntelliSense, type checking, and code navigation.

```json
"extensions": [
    "ms-python.vscode-pylance"
],
"settings": {
    "python.analysis.typeCheckingMode": "strict",
    "python.analysis.diagnosticMode": "workspace",
    "python.analysis.diagnosticSeverityOverrides": {
        "reportUnknownParameterType": false,
        "reportUnknownArgumentType": false,
        "reportUnknownVariableType": false,
        "reportUnknownMemberType": false,
        "reportMissingParameterType": false,
        "reportMissingTypeArgument": false,
        "reportGeneralTypeIssues": false
    },
    "python.analysis.completeFunctionParens": true,
    "python.analysis.inlayHints.variableType": true,
    "python.analysis.inlayHints.functionReturnTypes": true,
    "python.analysis.inlayHints.callArgumentName": true,
    "python.analysis.inlayHints.pytestParameters": true,
},
```

## 📊 Code Coverage Visualization

The DevContainer includes visual code coverage display that works with both VS Code's built-in test runner and task-based testing. Below shows an if statement being only partially covered by tests. The `if` line shows orange for partially covered by unit tests and the `print` line shows red for not covered.

![Preview Code Coverage in Editor](./docs/preview_coverage_in_editor.gif)

### How Coverage Works

Both testing methods generate a `coverage.xml` file in the workspace root:

- **VS Code UI Tests**: Run tests via the Testing panel (sidebar) → automatically generates `coverage.xml`
- **VS Code Tasks**: Run `pytest and coverage` task → generates `coverage.xml` and `JUNIT-TEST.xml`

### Coverage Gutter Extension

Visual code coverage display shows covered/uncovered lines directly in editor gutters.

**Setup:**
1. Run tests with coverage using either method above
2. Open Command Palette: `Ctrl+Shift+P`
3. Run: "Coverage Gutter: Display Coverage"
4. **Enable Watch Mode**: "Coverage Gutter: Watch" for automatic updates when `coverage.xml` changes

**Benefits:**
- **Real-time Feedback**: See coverage immediately after running tests
- **Visual Indicators**: Green (covered), red (uncovered), yellow (partial) line highlighting
- **Automatic Updates**: Watch mode refreshes coverage when you re-run tests

**Troubleshooting:**
- Ensure `coverage.xml` is generated in workspace root
- Check `.coveragerc` configuration if coverage isn't working
- Verify pytest is configured with coverage in `pytest.ini`

## 🔧 VS Code Tasks - Complete CI/CD Pipeline Integration

This DevContainer includes a comprehensive task automation system that mirrors your CI/CD pipeline, allowing you to validate your entire codebase locally before pushing to remote repositories.

### Why Use Tasks?

**Repository-Wide Analysis**: Unlike live editor feedback (limited to open files), tasks scan your entire codebase for comprehensive validation.  
**Pre-Push Confidence**: Catch issues locally using the same tools and configurations as your production CI/CD pipeline.  
**Parallel Execution**: Independent tasks can run simultaneously for faster feedback.

!["Run all Code Quality tools as VSCode Tasks"](./docs/run_all_vscode_tasks.gif)

### How to Run Tasks

**Command Palette**: `Ctrl+Shift+P` → `"Tasks: Run Task"` → Select task  
**Terminal Menu**: `Terminal` → `Run Task...`

### Available Tasks

#### 🏗️ Build Tasks
- **poetry lock**: Lock dependencies
- **poetry install**: Install dependencies  
- **build package**: Create Python wheel
- **install package**: Install the built package

#### ✅ Test Tasks  
- **sphinx build docs**: Generate documentation
- **ruff formatter**: Format code with Ruff
- **ruff linter**: Lint with Ruff
- **pylint**: Lint with Pylint
- **flake8**: Lint with Flake8
- **mypy**: Type check with MyPy
- **pyright**: Type check with Pyright
- **bandit**: Security scan with Bandit
- **semgrep**: Security scan with Semgrep
- **trufflehog**: Secret detection
- **pytest and coverage**: Run tests with coverage

### Results & Output
- **Terminal Panels**: Each task runs in a dedicated terminal
- **Error Indicator**: Red text with ❌ for failures
- **Generated Reports**: `coverage.xml`, `JUNIT-TEST.xml` in workspace root

### Technical Implementation

All tasks execute shell scripts from `.devcontainer/.dotfiles/python/scripts/` using centralized configuration files. This ensures consistency between local development and CI/CD environments.

### Configuration File Paths

All tools reference centralized configuration files:
```
.devcontainer/.dotfiles/python/
└── scripts/              # Execution scripts
    ├── ruff_formatter.sh
    ├── pylint.sh
    └── ...
├── ruff.toml            # Ruff configuration
├── mypy.ini             # MyPy type checker settings
├── pytest.ini           # Pytest configuration
├── ...
```

### Adding New Tasks

To add a new tool:

1. **Add configuration**: Create tool config file in `.dotfiles/`
2. **Create script**: Add execution script to `.dotfiles/.../scripts/` directory
3. **Define task**: Add task definition to `devcontainer.json`
4. **Update dependencies**: Add to appropriate task chain

### Task Debugging

For troubleshooting task execution:
- Check script permissions (`chmod +x`)
- Verify file paths and variable substitution
- Test scripts manually in terminal
- Review task output in dedicated terminal panels

This comprehensive task system provides a robust foundation for maintaining code quality and ensuring CI/CD pipeline compatibility.

## ⚙️ Platform Configuration

#### Git Configuration (Recommended)
Add the following lines to `.gitattributes` file in project root. This prevents git from thinking ALL files changed due to different line endings when opening devcontainer from Windows machines:
```bash
* text=auto
*.sh text eol=lf
*.conf text eol=lf
```

#### WSL2 Resource Limits (Optional)
Dev containers use WSL2 on Windows, which can consume significant resources. You can define limits in `.wslconfig` and place it at `C:\Users\<username>\.wslconfig`:

```ini
[wsl2]
# Limits VM memory to use no more than 6 GB
memory=6GB 
# Sets the VM to use four virtual processors
processors=4
# Sets amount of swap storage space to 8GB, default is 25% of available RAM
swap=8GB
# Enable page reporting so WSL releases memory back to Windows when free
pageReporting=true
```

---

## 🚀 Take Your Development Further

**Scale to Production:** Deploy with the [Azure DevOps CI/CD templates](https://github.com/KrijnvanderBurg/.azuredevops) that use identical configurations. **25+ pipeline templates** ensure seamless local-to-production workflows with zero surprises.

**Understand the Magic:** Explore the [shared configuration architecture](https://github.com/KrijnvanderBurg/.dotfiles) that makes local-remote consistency possible. See how **one script, one config** eliminates environment drift.

**Complete Integration:** Check out the [full DevOps Toolkit](https://github.com/KrijnvanderBurg/DevOps-Toolkit) for the complete picture of how DevContainers, CI/CD, and shared configs create a **unified development experience**.
