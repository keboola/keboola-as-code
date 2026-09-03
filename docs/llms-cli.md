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

To install the Keboola CLI, follow the instructions in the official documentation: [user-docs/installation.md](user-docs/installation.md)

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
    Refer to: [user-docs/commands/sync/init.md](user-docs/commands/sync/init.md)

2.  **API Token:** You will be prompted to enter your Keboola Storage API host and API token.  The API token is essential for authenticating with your Keboola project.  It's recommended to store the API token securely, for example, using environment variables or a secrets management system.
    *   The CLI creates a `.env.local` file to store the API token locally. **Keep this file secret and do not commit it to version control.**
3.  **Project Structure:** After initialization, the local directory will mirror the structure of your Keboola project. Key files and directories include:
    *   `.keboola/manifest.json`:  Project manifest file that tracks local and remote object IDs and paths.
    *   `main/`, `bar/`, ...: Directories representing Keboola branches (e.g., `main` for the main branch, `bar` for a development branch).
    *   `extractor/`, `transformation/`, `writer/`, `other/`, `app/`, `_shared/`, `variables/`, `schedules/`: Directories within branches representing different component types.
    *   Configuration files (`config.json`), metadata files (`meta.json`), and description files (`description.md`) for each Keboola object (configuration, config row, shared code, etc.).
    *   Transformation code files (e.g., `.sql`, `.py`).
    Refer to: [user-docs/getting-started.md](user-docs/getting-started.md) and [user-docs/structure.md](user-docs/structure.md)
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
    [user-docs/commands/status.md](user-docs/commands/status.md)
### 2. `sync` - Synchronization Commands
These commands manage the synchronization between the local directory and the remote Keboola project.
*   `kbc sync init`: Initializes a new project (as described in Getting Started).
    [user-docs/commands/sync/init.md](user-docs/commands/sync/init.md)
*   `kbc sync pull`: Downloads changes from the Keboola project to the local directory.
    [user-docs/commands/sync/pull.md](user-docs/commands/sync/pull.md)
*   `kbc sync push`: Uploads local changes to the Keboola project.
    [user-docs/commands/sync/push.md](user-docs/commands/sync/push.md)
*   `kbc sync diff`: Shows the differences between the local directory and the Keboola project.
    [user-docs/commands/sync/diff.md](user-docs/commands/sync/diff.md)
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
    *   `kbc local create config <component-type> <component-id> <config-name>`: Creates a new configuration. [user-docs/commands/local/create/config.md](user-docs/commands/local/create/config.md)
    *   `kbc local create row <component-type> <component-id> <config-name> <row-name>`: Creates a new config row. [user-docs/commands/local/create/row.md](user-docs/commands/local/create/row.md)
*   `kbc local persist`:  Persists changes you have made to local files (for example newly created configurations or rows) by recording them in the project manifest. Typically, "persist" is called under the hood by other "local" commands (like create, fix-paths, etc.). However, if you have manually created or moved files and need the manifest to reflect these new paths, "local persist" ensures that the local directory structure and manifest remain consistent.
    *   Example:
      ```bash
      kbc local persist 
      ```
      // Will update .keboola/manifest.json to reflect any new or moved files.
*   `kbc local encrypt`: Encrypts values in configuration files locally before pushing them to the project. [user-docs/commands/local/encrypt.md](user-docs/commands/local/encrypt.md)
*   `kbc local validate`: Validates the local project directory against Keboola schema and rules.
    *   `kbc local validate config`: Validates configuration files. [user-docs/commands/local/validate/config.md](user-docs/commands/local/validate/config.md)
    *   `kbc local validate row`: Validates config row files. [user-docs/commands/local/validate/row.md](user-docs/commands/local/validate/row.md)
    *   `kbc local validate schema`: Validates schemas defined in configuration files. [user-docs/commands/local/validate/schema.md](user-docs/commands/local/validate/schema.md)
*   `kbc local fix-paths`:  Automatically fixes paths in the local project to adhere to Keboola's naming conventions. [user-docs/commands/local/fix-paths.md](user-docs/commands/local/fix-paths.md)
*   `kbc local template`: Commands for working with templates locally.
    *   `kbc local template delete <template-instance-id>`: Deletes a template instance.
    *   `kbc local template list`: Lists used templates in the project.
    *   `kbc local template use <repository-name> <template-id> <version>`: Applies a template to the project.

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

*   `kbc remote create`: Commands for creating remote Keboola objects. [user-docs/commands/remote/create.md](user-docs/commands/remote/create.md)
    *   `kbc remote create branch <branch-name>`: Creates a new branch. [user-docs/commands/remote/create/branch.md](user-docs/commands/remote/create/branch.md)
    *   `kbc remote create bucket <bucket-name> <stage>`: Creates a new storage bucket. For non-interactive usage, the following parameters are required:
        ```bash
        kbc remote create bucket --stage <in|out> --display-name <bucket-display-name> --name <bucket-name> --description <bucket-description>
        ```
        **Important**: This command must be run from within an initialized Keboola project directory (where `.env.local` exists) to work in non-interactive mode.
        [user-docs/commands/remote/create/bucket.md](user-docs/commands/remote/create/bucket.md)
*   `kbc remote file`: Commands for managing files in Keboola Storage. [user-docs/commands/remote/file.md](user-docs/commands/remote/file.md)
    *   `kbc remote file download <file-id> <destination-path>`: Downloads a file from storage. [user-docs/commands/remote/file/download.md](user-docs/commands/remote/file/download.md)
    *   `kbc remote file upload <source-path> <destination-bucket> [options]`: Uploads a file to storage. [user-docs/commands/remote/file/upload.md](user-docs/commands/remote/file/upload.md)
*   `kbc remote job`: Commands for managing jobs. [user-docs/commands/remote/job.md](user-docs/commands/remote/job.md)
    *   `kbc remote job run <component-id> <configuration-id> [flags]`: Runs a job for a specific configuration. [user-docs/commands/remote/job/run.md](user-docs/commands/remote/job/run.md)
*   `kbc remote table`: Commands for managing tables in Keboola Storage. [user-docs/commands/remote/table.md](user-docs/commands/remote/table.md)
    *   `kbc remote table create <table-name> <bucket-id> [options]`: Creates a new table. [user-docs/commands/remote/table/create.md](user-docs/commands/remote/table/create.md)
    *   `kbc remote table upload <source-path> <table-id> [options]`: Uploads data to a table. [user-docs/commands/remote/table/upload.md](user-docs/commands/remote/table/upload.md)
    *   `kbc remote table download <table-id> <destination-path> [options]`: Downloads data from a table. [user-docs/commands/remote/table/download.md](user-docs/commands/remote/table/download.md)
    *   `kbc remote table preview <table-id> [options]`: Shows a preview of a table. [user-docs/commands/remote/table/preview.md](user-docs/commands/remote/table/preview.md)
    *   `kbc remote table detail <table-id>`: Shows details of a table. [user-docs/commands/remote/table/detail.md](user-docs/commands/remote/table/detail.md)
    *   `kbc remote table import <source-path> <table-id> [options]`: Imports data to a table. [user-docs/commands/remote/table/import.md](user-docs/commands/remote/table/import.md)
    *   `kbc remote table unload <table-id> <destination-path> [options]`: Unloads data from a table to a file. [user-docs/commands/remote/table/unload.md](user-docs/commands/remote/table/unload.md)
*   `kbc remote workspace`: Commands for managing workspaces. [user-docs/commands/remote/workspace.md](user-docs/commands/remote/workspace.md)
    *   `kbc remote workspace create <type>`: Creates a new workspace. [user-docs/commands/remote/workspace/create.md](user-docs/commands/remote/workspace/create.md)
    *   `kbc remote workspace delete <workspace-id>`: Deletes a workspace. [user-docs/commands/remote/workspace/delete.md](user-docs/commands/remote/workspace/delete.md)
    *   `kbc remote workspace detail <workspace-id>`: Shows details of a workspace. [user-docs/commands/remote/workspace/detail.md](user-docs/commands/remote/workspace/detail.md)
    *   `kbc remote workspace list`: Lists workspaces. [user-docs/commands/remote/workspace/list.md](user-docs/commands/remote/workspace/list.md)

### 5. `template` - Template Commands

These commands are used for managing templates and template repositories.

*   `kbc template repository`: Commands for managing template repositories.
    *   `kbc template repository init`: Initializes a new template repository in the local directory.
*   `kbc template create`: Creates a new template in a template repository.
*   `kbc template describe <repository-name> <template-id> <version>`: Describes a template.
*   `kbc template list [repository-name]`: Lists templates in a repository or all repositories.
*   `kbc template test`: Commands for testing templates.
    *   `kbc template test create`: Creates template tests.
    *   `kbc template test run`: Runs template tests.

### 6. `ci` - CI/CD Commands

*   `kbc ci workflows`: Generates CI workflow files for GitHub Actions to automate project validation, pushing, and pulling. [user-docs/commands/ci/workflows.md](user-docs/commands/ci/workflows.md)

### 7. `dbt` - DBT Commands

Commands for integrating with DBT (Data Build Tool).

*   `kbc dbt init`: Initializes DBT project files within the Keboola project directory. [user-docs/commands/dbt/init.md](user-docs/commands/dbt/init.md)
*   `kbc dbt generate`: Generates DBT-related files. [user-docs/commands/dbt/generate.md](user-docs/commands/dbt/generate.md)
    *   `kbc dbt generate profile`: Generates a DBT profile for Keboola. [user-docs/commands/dbt/generate/profile.md](user-docs/commands/dbt/generate/profile.md)
    *   `kbc dbt generate sources`: Generates DBT sources from Keboola tables. [user-docs/commands/dbt/generate/sources.md](user-docs/commands/dbt/generate/sources.md)
    *   `kbc dbt generate env`: Generates a `.env` file for DBT. [user-docs/commands/dbt/generate/env.md](user-docs/commands/dbt/generate/env.md)

## DevOps Use Cases and GitHub Integration

The Keboola CLI is designed to be integrated into DevOps workflows. Key use cases include:

*   **Automated deployments:** Use CI/CD pipelines (e.g., GitHub Actions) to automatically deploy changes to Keboola projects whenever code is pushed to a Git repository.
*   **Testing and validation:**  Automate validation of configurations and templates.
*   **Environment promotion:**  Manage different Keboola environments (development, staging, production) using branches and CI/CD.
*   **Disaster recovery:**  Version control and synchronization ensure that project configurations can be easily restored.

Refer to: [user-docs/devops-use-cases.md](user-docs/devops-use-cases.md) and [user-docs/github-integration.md](user-docs/github-integration.md)

## Templates and Reusability

Keboola Templates allow you to create reusable configurations. The CLI provides commands to:

*   Create templates from existing projects or from scratch.
*   Use templates to quickly set up new configurations.
*   Manage template repositories.
*   Test templates.

## Important Notes for AI Agents

*   **API Token Security:**  Handle the Keboola Storage API token with extreme care. Do not expose it in public logs or commit it to version control. Use secure methods for storing and accessing the token.
*   **Working Directory:**  Always ensure commands are executed from within the initialized Keboola project directory where `.env.local` exists. Commands will switch to interactive mode if they cannot find the credentials file, even when using non-interactive parameters.
*   **Manifest File Integrity:**  Avoid manual modifications of the `.keboola/manifest.json` file unless absolutely necessary and with a thorough understanding of its structure. Incorrect modifications can lead to data loss or synchronization issues.
*   **Error Handling:** Implement robust error handling when using the CLI. Check command exit codes and parse output for errors.
*   **Non-interactive Mode:**  For automated tasks, use the CLI in non-interactive mode by providing all necessary parameters as command flags. This is crucial for CI/CD pipelines and scripts.
*   **Documentation is Key:**  Always refer to the official Keboola CLI documentation ([user-docs/README.md](user-docs/README.md)) for the most up-to-date information on commands, options, and best practices.
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
