# .devcontainer
This folder contains several specialized DevContainers for different development environments.

> **Note**: The `.devcontainer` folder might be invisible in some file explorers due to the leading dot (`.`) in its name. Ensure your file explorer is configured to show hidden files and folders.

## Available Containers
### [Python/Spark -Code Quality- DevContainer](./python-spark)
A complete Python development environment with:
- Python workflow and Poetry package manager
- Comprehensive linting and static analysis tools (Ruff, PyLint, Flake8, MyPy)
- Code quality tools (Bandit, Semgrep)
- Testing configuration with pytest and coverage reporting
- Pre-configured VS Code tasks for all development workflows

## Quick Installation
**Prerequisites**:
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- [VS Code](https://code.visualstudio.com/) with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) installed

**Build and start the DevContainer**:
- Press `F1` to open the command palette
- Type and select `Dev Containers: Rebuild and Reopen in Container`
- VS Code will build the Docker images and start the containers defined in `docker-compose.yml`
- This process may take several minutes the first time

### How It Works: Pylint example
When you run a tool locally in VS Code it uses a script and config file from `.dotfiles`.
```jsonc
// Example VSCode Task
{
   "label": "pylint",
   //              The Nested Submodule .dotfiles  ⤵
   "command": "${workspaceFolder}/scripts/pylint.sh", // ← Same script
   "args": [
      "${workspaceFolder}/src",                                 // ← Same target dirpath
      "--config ${workspaceFolder}/.github/config/.pylintrc"    // ← Same config file
   ],
   ...
}
```

When the CI/CD pipeline runs the same tool it uses the same script and config file from `.github/config/`:
```js
// Example Azure Devops pipeline step
steps:
- script: |
    //               The Nested Submodule .dotfiles  ⤵
    sh $(Build.Repository.LocalPath)/.github/scripts/pylint.sh \ // ← Same script
      $(Build.Repository.LocalPath)/src \                             // ← Same dirpath
      --config $(Build.Repository.LocalPath)/.github/python/.pylintrc // ← Same config
  displayName: Pylint (linter)
  ...
```

This ensures that **quality gates in CI/CD match the local experience exactly**; eliminating waiting for CICD runs to see if it conforms to set quality gates.
