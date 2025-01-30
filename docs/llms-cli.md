# Keboola CLI for AI Agents

This document provides a summary of the Keboola CLI (Command Line Interface) for AI agents. It outlines the CLI's purpose, key features, commands, and how it can be used to interact with Keboola projects.

## Overview

The Keboola CLI is a powerful tool for managing Keboola projects as code. It allows you to:

*   **Represent Keboola projects locally:**  Synchronize a Keboola project to a local directory, enabling version control and offline work.
*   **Bidirectional Synchronization:** Push local changes to a Keboola project and pull remote changes to your local directory.
*   **Automate Keboola tasks:**  Execute various operations on Keboola projects, such as creating configurations, running jobs, managing templates, and more, directly from the command line.
*   **Integrate with DevOps workflows:** Generate CI/CD pipelines for Keboola projects.
*   **Manage Templates:** Create, use, and manage templates for repeatable Keboola configurations.
*   **Work with DBT:** Integrate with DBT (Data Build Tool) for data transformations within Keboola.

**Key Features:**

*   **Configuration as Code:**  Manage Keboola configurations (extractors, transformations, writers, etc.) as code in JSON and other formats.
*   **Version Control:**  Use Git to version control your Keboola project configurations.
*   **Automation:**  Automate deployments, testing, and other Keboola-related tasks.
*   **Extensibility:**  Extend Keboola functionality through templates and DBT.
*   **Interactive and Non-interactive Modes:** Use the CLI interactively with dialogs or non-interactively with command flags for automation.

## Installation

To install the Keboola CLI, follow the instructions in the official documentation: [https://developers.keboola.com/cli/installation/](https://developers.keboola.com/cli/installation/)

Installation methods include:

*   **Homebrew (macOS and Linux)**
*   **Debian and RPM packages**
*   **Standalone binary (macOS, Linux, Windows)**
*   **Docker**

## Getting Started

1.  **Initialization:** Start by running `kbc init` in an empty directory. This command initializes a new local project directory and synchronizes it with a Keboola project.
    ```bash
    kbc init
    ```
    Refer to: [https://developers.keboola.com/cli/commands/sync/init/](https://developers.keboola.com/cli/commands/sync/init/)

2.  **API Token:** You will be prompted to enter your Keboola Storage API host and API token.  The API token is essential for authenticating with your Keboola project.  It's recommended to store the API token securely, for example, using environment variables or a secrets management system.
    *   The CLI creates a `.env.local` file to store the API token locally. **Keep this file secret and do not commit it to version control.**
3.  **Project Structure:** After initialization, the local directory will mirror the structure of your Keboola project. Key files and directories include:
    *   `.keboola/manifest.json`:  Project manifest file that tracks local and remote object IDs and paths.
    *   `main/`, `bar/`, ...: Directories representing Keboola branches (e.g., `main` for the main branch, `bar` for a development branch).
    *   `extractor/`, `transformation/`, `writer/`, `other/`, `app/`, `_shared/`, `variables/`, `schedules/`: Directories within branches representing different component types.
    *   Configuration files (`config.json`), metadata files (`meta.json`), and description files (`description.md`) for each Keboola object (configuration, config row, shared code, etc.).
    *   Transformation code files (e.g., `.sql`, `.py`).
    Refer to: [https://developers.keboola.com/cli/getting-started/](https://developers.keboola.com/cli/getting-started/) and [https://developers.keboola.com/cli/structure/](https://developers.keboola.com/cli/structure/)
## Core Concepts
*   **Manifest:** The `.keboola/manifest.json` file is crucial for the CLI's operation. It maintains the mapping between local file paths and remote Keboola object IDs. **Do not modify this file manually unless you understand its structure.**
*   **Branches:** Keboola projects can have multiple branches for development, testing, and production. The CLI works within a specific branch context.
*   **Components:** Keboola components (extractors, transformations, writers, etc.) are managed as configurations within the project.
*   **Configurations and Config Rows:**  Configurations define the settings for components. Config rows are used for more granular settings within configurations.
*   **Shared Code:** Reusable code snippets that can be shared across transformations.
*   **Templates:** Pre-built Keboola configurations that can be easily applied to projects.
*   **Workspaces:** Isolated environments for data processing and transformations.

## Command Categories and Usage

The Keboola CLI commands are organized into categories:

### 1. `status`

*   `kbc status`: Shows information about the current working directory and the Keboola project it's connected to.
    [https://developers.keboola.com/cli/commands/status/](https://developers.keboola.com/cli/commands/status/)
### 2. `sync` - Synchronization Commands
These commands manage the synchronization between the local directory and the remote Keboola project.
*   `kbc sync init`: Initializes a new project (as described in Getting Started).
    [https://developers.keboola.com/cli/commands/sync/init/](https://developers.keboola.com/cli/commands/sync/init/)
*   `kbc sync pull`: Downloads changes from the Keboola project to the local directory.
    [https://developers.keboola.com/cli/commands/sync/pull/](https://developers.keboola.com/cli/commands/sync/pull/)
*   `kbc sync push`: Uploads local changes to the Keboola project.
    [https://developers.keboola.com/cli/commands/sync/push/](https://developers.keboola.com/cli/commands/sync/push/)
*   `kbc sync diff`: Shows the differences between the local directory and the Keboola project.
    [https://developers.keboola.com/cli/commands/sync/diff/](https://developers.keboola.com/cli/commands/sync/diff/)
**Typical Workflow:**
1.  `kbc sync pull` - to get the latest changes from the remote project.
2.  Make local changes (edit configurations, code, etc.).
3.  `kbc sync diff` - to review changes before pushing.
4.  `kbc sync push` - to upload changes to the remote project.
### Interactive vs. Non-interactive Command Usage
Some commands (e.g., "local create") can be run without fully specifying all arguments. In such a case, the CLI will prompt you interactively to fill in missing information (such as selecting a branch, specifying component IDs, and so on). This means you can choose:
1. Pass command flags (non-interactive):
   ```bash
   # Example with all required parameters for non-interactive mode
   kbc local create config extractor keboola.ex-db-mysql "My Local DB Config" --branch="Main"
   
   # Example of bucket creation with all required parameters
   kbc remote create bucket --stage in --display-name "My Bucket" --name "my-bucket" --description "My bucket description"
   ```
2. Omit certain arguments (interactive):
   ```bash
   kbc local create config
   # The CLI will then prompt you to select the component and to name the config
   
   kbc remote create bucket
   # The CLI will prompt for stage, name, display name, and description
   ```
**Important**: For non-interactive mode to work:
- All required parameters must be specified via command flags
- Commands must be run from within an initialized Keboola project directory (where `.env.local` exists)
- If either condition is not met, the command will switch to interactive mode
This approach applies similarly to commands that allow you to create config rows, choose branches, or pick from multiple options.
### 3. `local` - Local Commands
These commands operate on the local project directory.
*   `kbc local create <component-type> <component-id> <config-name>`: Creates a new configuration (or config row).
    *   `kbc local create config <component-type> <component-id> <config-name>`: Creates a new configuration. [https://developers.keboola.com/cli/commands/local/create/config/](https://developers.keboola.com/cli/commands/local/create/config/)
    *   `kbc local create row <component-type> <component-id> <config-name> <row-name>`: Creates a new config row. [https://developers.keboola.com/cli/commands/local/create/row/](https://developers.keboola.com/cli/commands/local/create/row/)
*   `kbc local persist`:  Persists changes you have made to local files (for example newly created configurations or rows) by recording them in the project manifest. Typically, "persist" is called under the hood by other "local" commands (like create, fix-paths, etc.). However, if you have manually created or moved files and need the manifest to reflect these new paths, "local persist" ensures that the local directory structure and manifest remain consistent.
    *   Example:
      ```bash
      kbc local persist 
      ```
      // Will update .keboola/manifest.json to reflect any new or moved files.
*   `kbc local encrypt`: Encrypts values in configuration files locally before pushing them to the project. [https://developers.keboola.com/cli/commands/local/encrypt/](https://developers.keboola.com/cli/commands/local/encrypt/)
*   `kbc local validate`: Validates the local project directory against Keboola schema and rules.
    *   `kbc local validate config`: Validates configuration files. [https://developers.keboola.com/cli/commands/local/validate/config/](https://developers.keboola.com/cli/commands/local/validate/config/)
    *   `kbc local validate row`: Validates config row files. [https://developers.keboola.com/cli/commands/local/validate/row/](https://developers.keboola.com/cli/commands/local/validate/row/)
    *   `kbc local validate schema`: Validates schemas defined in configuration files. [https://developers.keboola.com/cli/commands/local/validate/schema/](https://developers.keboola.com/cli/commands/local/validate/schema/)
*   `kbc local fix-paths`:  Automatically fixes paths in the local project to adhere to Keboola's naming conventions. [https://developers.keboola.com/cli/commands/local/fix-paths/](https://developers.keboola.com/cli/commands/local/fix-paths/)
*   `kbc local template`: Commands for working with templates locally. [https://developers.keboola.com/cli/commands/local/template/](https://developers.keboola.com/cli/commands/local/template/)
    *   `kbc local template delete <template-instance-id>`: Deletes a template instance. [https://developers.keboola.com/cli/commands/local/template/delete/](https://developers.keboola.com/cli/commands/local/template/delete/)
    *   `kbc local template list`: Lists used templates in the project. [https://developers.keboola.com/cli/commands/local/template/list/](https://developers.keboola.com/cli/commands/local/template/list/)
    *   `kbc local template use <repository-name> <template-id> <version>`: Applies a template to the project. [https://developers.keboola.com/cli/commands/local/template/use/](https://developers.keboola.com/cli/commands/local/template/use/)

### Examples of Local Commands with Flags

Below are some simplified examples of non-interactive usage that specify all needed flags:

1) Create a new config for the MySQL extractor in the "dev" branch:
   ```bash
   kbc local create config extractor keboola.ex-db-mysql "My New Config" --branch=dev
   ```
2) Create a new config row named "Row1" in an existing config called "My New Config":
   ```bash
   kbc local create row extractor keboola.ex-db-mysql "My New Config" "Row1" --branch=dev
   ```
### 4. `remote` - Remote Commands

These commands interact directly with the remote Keboola project.

*   `kbc remote create`: Commands for creating remote Keboola objects. [https://developers.keboola.com/cli/commands/remote/create/](https://developers.keboola.com/cli/commands/remote/create/)
    *   `kbc remote create branch <branch-name>`: Creates a new branch. [https://developers.keboola.com/cli/commands/remote/create/branch/](https://developers.keboola.com/cli/commands/remote/create/branch/)
    *   `kbc remote create bucket <bucket-name> <stage>`: Creates a new storage bucket. For non-interactive usage, the following parameters are required:
        ```bash
        kbc remote create bucket --stage <in|out> --display-name <bucket-display-name> --name <bucket-name> --description <bucket-description>
        ```
        **Important**: This command must be run from within an initialized Keboola project directory (where `.env.local` exists) to work in non-interactive mode.
        [https://developers.keboola.com/cli/commands/remote/create/bucket/](https://developers.keboola.com/cli/commands/remote/create/bucket/)
*   `kbc remote file`: Commands for managing files in Keboola Storage. [https://developers.keboola.com/cli/commands/remote/file/](https://developers.keboola.com/cli/commands/remote/file/)
    *   `kbc remote file download <file-id> <destination-path>`: Downloads a file from storage. [https://developers.keboola.com/cli/commands/remote/file/download/](https://developers.keboola.com/cli/commands/remote/file/download/)
    *   `kbc remote file upload <source-path> <destination-bucket> [options]`: Uploads a file to storage. [https://developers.keboola.com/cli/commands/remote/file/upload/](https://developers.keboola.com/cli/commands/remote/file/upload/)
*   `kbc remote job`: Commands for managing jobs. [https://developers.keboola.com/cli/commands/remote/job/](https://developers.keboola.com/cli/commands/remote/job/)
    *   `kbc remote job run <component-id> <configuration-id> [flags]`: Runs a job for a specific configuration. [https://developers.keboola.com/cli/commands/remote/job/run/](https://developers.keboola.com/cli/commands/remote/job/run/)
*   `kbc remote table`: Commands for managing tables in Keboola Storage. [https://developers.keboola.com/cli/commands/remote/table/](https://developers.keboola.com/cli/commands/remote/table/)
    *   `kbc remote table create <table-name> <bucket-id> [options]`: Creates a new table. [https://developers.keboola.com/cli/commands/remote/table/create/](https://developers.keboola.com/cli/commands/remote/table/create/)
    *   `kbc remote table upload <source-path> <table-id> [options]`: Uploads data to a table. [https://developers.keboola.com/cli/commands/remote/table/upload/](https://developers.keboola.com/cli/commands/remote/table/upload/)
    *   `kbc remote table download <table-id> <destination-path> [options]`: Downloads data from a table. [https://developers.keboola.com/cli/commands/remote/table/download/](https://developers.keboola.com/cli/commands/remote/table/download/)
    *   `kbc remote table preview <table-id> [options]`: Shows a preview of a table. [https://developers.keboola.com/cli/commands/remote/table/preview/](https://developers.keboola.com/cli/commands/remote/table/preview/)
    *   `kbc remote table detail <table-id>`: Shows details of a table. [https://developers.keboola.com/cli/commands/remote/table/detail/](https://developers.keboola.com/cli/commands/remote/table/detail/)
    *   `kbc remote table import <source-path> <table-id> [options]`: Imports data to a table. [https://developers.keboola.com/cli/commands/remote/table/import/](https://developers.keboola.com/cli/commands/remote/table/import/)
    *   `kbc remote table unload <table-id> <destination-path> [options]`: Unloads data from a table to a file. [https://developers.keboola.com/cli/commands/remote/table/unload/](https://developers.keboola.com/cli/commands/remote/table/unload/)
*   `kbc remote workspace`: Commands for managing workspaces. [https://developers.keboola.com/cli/commands/remote/workspace/](https://developers.keboola.com/cli/commands/remote/workspace/)
    *   `kbc remote workspace create <type>`: Creates a new workspace. [https://developers.keboola.com/cli/commands/remote/workspace/create/](https://developers.keboola.com/cli/commands/remote/workspace/create/)
    *   `kbc remote workspace delete <workspace-id>`: Deletes a workspace. [https://developers.keboola.com/cli/commands/remote/workspace/delete/](https://developers.keboola.com/cli/commands/remote/workspace/delete/)
    *   `kbc remote workspace detail <workspace-id>`: Shows details of a workspace. [https://developers.keboola.com/cli/commands/remote/workspace/detail/](https://developers.keboola.com/cli/commands/remote/workspace/detail/)
    *   `kbc remote workspace list`: Lists workspaces. [https://developers.keboola.com/cli/commands/remote/workspace/list/](https://developers.keboola.com/cli/commands/remote/workspace/list/)

### 5. `template` - Template Commands

These commands are used for managing templates and template repositories.

*   `kbc template repository`: Commands for managing template repositories. [https://developers.keboola.com/cli/commands/template/repository/](https://developers.keboola.com/cli/commands/template/repository/)
    *   `kbc template repository init`: Initializes a new template repository in the local directory. [https://developers.keboola.com/cli/commands/template/repository/init/](https://developers.keboola.com/cli/commands/template/repository/init/)
*   `kbc template create`: Creates a new template in a template repository. [https://developers.keboola.com/cli/commands/template/create/](https://developers.keboola.com/cli/commands/template/create/)
*   `kbc template describe <repository-name> <template-id> <version>`: Describes a template. [https://developers.keboola.com/cli/commands/template/describe/](https://developers.keboola.com/cli/commands/template/describe/)
*   `kbc template list [repository-name]`: Lists templates in a repository or all repositories. [https://developers.keboola.com/cli/commands/template/list/](https://developers.keboola.com/cli/commands/template/list/)
*   `kbc template test`: Commands for testing templates. [https://developers.keboola.com/cli/commands/template/test/](https://developers.keboola.com/cli/commands/template/test/)
    *   `kbc template test create`: Creates template tests. [https://developers.keboola.com/cli/commands/template/test/create/](https://developers.keboola.com/cli/commands/template/test/create/)
    *   `kbc template test run`: Runs template tests. [https://developers.keboola.com/cli/commands/template/test/run/](https://developers.keboola.com/cli/commands/template/test/run/)

### 6. `ci` - CI/CD Commands

*   `kbc ci workflows`: Generates CI workflow files for GitHub Actions to automate project validation, pushing, and pulling. [https://developers.keboola.com/cli/commands/ci/workflows/](https://developers.keboola.com/cli/commands/ci/workflows/)

### 7. `dbt` - DBT Commands

Commands for integrating with DBT (Data Build Tool).

*   `kbc dbt init`: Initializes DBT project files within the Keboola project directory. [https://developers.keboola.com/cli/commands/dbt/init/](https://developers.keboola.com/cli/commands/dbt/init/)
*   `kbc dbt generate`: Generates DBT-related files. [https://developers.keboola.com/cli/commands/dbt/generate/](https://developers.keboola.com/cli/commands/dbt/generate/)
    *   `kbc dbt generate profile`: Generates a DBT profile for Keboola. [https://developers.keboola.com/cli/commands/dbt/generate/profile/](https://developers.keboola.com/cli/commands/dbt/generate/profile/)
    *   `kbc dbt generate sources`: Generates DBT sources from Keboola tables. [https://developers.keboola.com/cli/commands/dbt/generate/sources/](https://developers.keboola.com/cli/commands/dbt/generate/sources/)
    *   `kbc dbt generate env`: Generates a `.env` file for DBT. [https://developers.keboola.com/cli/commands/dbt/generate/env/](https://developers.keboola.com/cli/commands/dbt/generate/env/)

## DevOps Use Cases and GitHub Integration

The Keboola CLI is designed to be integrated into DevOps workflows. Key use cases include:

*   **Automated deployments:** Use CI/CD pipelines (e.g., GitHub Actions) to automatically deploy changes to Keboola projects whenever code is pushed to a Git repository.
*   **Testing and validation:**  Automate validation of configurations and templates.
*   **Environment promotion:**  Manage different Keboola environments (development, staging, production) using branches and CI/CD.
*   **Disaster recovery:**  Version control and synchronization ensure that project configurations can be easily restored.

Refer to: [https://developers.keboola.com/cli/devops-use-cases/](https://developers.keboola.com/cli/devops-use-cases/) and [https://developers.keboola.com/cli/github-integration/](https://developers.keboola.com/cli/github-integration/)

## Templates and Reusability

Keboola Templates allow you to create reusable configurations. The CLI provides commands to:

*   Create templates from existing projects or from scratch.
*   Use templates to quickly set up new configurations.
*   Manage template repositories.
*   Test templates.

Refer to: [https://developers.keboola.com/cli/templates/](https://developers.keboola.com/cli/templates/) and [https://developers.keboola.com/cli/templates/tutorial/](https://developers.keboola.com/cli/templates/tutorial/)

## Important Notes for AI Agents

*   **API Token Security:**  Handle the Keboola Storage API token with extreme care. Do not expose it in public logs or commit it to version control. Use secure methods for storing and accessing the token.
*   **Working Directory:**  Always ensure commands are executed from within the initialized Keboola project directory where `.env.local` exists. Commands will switch to interactive mode if they cannot find the credentials file, even when using non-interactive parameters.
*   **Manifest File Integrity:**  Avoid manual modifications of the `.keboola/manifest.json` file unless absolutely necessary and with a thorough understanding of its structure. Incorrect modifications can lead to data loss or synchronization issues.
*   **Error Handling:** Implement robust error handling when using the CLI. Check command exit codes and parse output for errors.
*   **Non-interactive Mode:**  For automated tasks, use the CLI in non-interactive mode by providing all necessary parameters as command flags. This is crucial for CI/CD pipelines and scripts.
*   **Documentation is Key:**  Always refer to the official Keboola CLI documentation ([https://developers.keboola.com/cli/](https://developers.keboola.com/cli/)) for the most up-to-date information on commands, options, and best practices.
*   **For inline help and usage details, run:**
    ```bash
    kbc --help  
    kbc <command> --help  
    ```
    This will display both short and long usage messages, along with any available flags or sub-commands.

This document provides a foundational understanding of the Keboola CLI for AI agents. By leveraging these commands and concepts, AI agents can effectively manage and automate Keboola projects.

## Contributing to the Keboola CLI Codebase (For AI Agents)

The Keboola CLI codebase is primarily located in the "internal/pkg/service/cli" directory. Here are some key points to help you navigate and contribute to the CLI code:

1. Project Structure:
   - "cmd" Package: The central entry point for CLI commands. Each major command category (e.g. sync, local, remote, etc.) has its own subpackage with additional subcommands.  
   - "helpmsg": Houses short and long descriptions for each CLI command. Command packages typically call "helpmsg.Read(...)" to load text displayed in --help.  
   - "dialog" and "prompt": Define how interactive prompts are handled, including multi-select, confirm, textual input, etc.  
   - "dependencies": Provides the dependency injection logic, creating "Provider" objects that supply project-related resources (manifest, dialogs, APIs, etc.) to commands.  
   - "pkg/lib/operation": Implements high-level operations (e.g. "Create Config") that commands invoke after collecting user inputs.

2. Rendered Command Help:
   - Short descriptions come from "helpmsg/short.txt" files.  
   - Long descriptions or usage instructions often come from "helpmsg/long.txt" files.  
   - This separation allows easily updating user-facing text without changing the command logic.

3. Typical Flow for a Command:
   - A user runs "kbc <some_command>".  
   - The associated subcommand in "cmd/<category>" is invoked.  
   - Command flags are bound with the "configmap" system, reading from CLI arguments or environment variables.  
   - Dependencies (manifest, logger, dialogs, project, environment) are loaded.  
   - The user may be prompted interactively if some arguments are missing.  
   - An operation from "pkg/lib/operation/..." is called, performing the actual logic, data updates, and/or network calls.

4. Testing:
   - Tests are spread across corresponding subdirectories in "cmd/..." for command-level tests and "pkg/lib/operation/..." for operation-level tests.  
   - Many tests utilize a "dialog.NewForTest(...)" system to simulate interactive prompts.  
   - Because the CLI needs both local file state (manifest) and remote calls, tests often rely on mocked or in-memory resources.

5. Contributing Code Changes:
   - Familiarize yourself with the relevant command subpackage (local, remote, template, etc.) and its related operation.  
   - Add or modify help messages in the "helpmsg/" files if user-facing text changes.  
   - Update or create tests that cover the new or modified behaviors. 

6. Tips for AI Agents Generating CLI Edits:
   - Keep user experience in mind by ensuring that interactive prompts and help messages remain consistent.  
   - When adding new flags or subcommands, reflect them in both "helpmsg" files and the "cmd" package code.  
   - Validate that changes do not break manifest integrity or fail with incomplete user input.

By understanding this structure and flow, you—or an AI agent—will be well-equipped to make informed modifications, add new features, or fix bugs in the Keboola CLI code.
