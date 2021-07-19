# Quickstart documentation
This document describes basic scenarious on how to use the CLI assuming the CLI has been already installed and ready to use.
## Init project directory
To start using CLI, a diretory has to be initialized locally with the configurations of the remote project, the following prerequisities are needed:
- **Master API Token** - go to the remote project settings -> API Tokens and click on your token(labeled as `Yours`). In the token detail page click on `Refresh` button and refresh the token. Copy the refreshed Master API token to the clipboard. 
- **Empty project directory(aka local directory)** - create an empty directory, name it whatever your like e.g. `mkdir my-kbc-project`

Call **`kbc init`** command from within the created empty project directory. The command asks for the host and Master API Token inputs. After the commands finishes the directory is initialized and contains all the configurations of the remote project, e.g.:

```
➜  my-kbc-project kbc init 
Please enter Keboola Storage API host, eg. "connection.keboola.com".
? API host connection.keboola.com
Please enter Keboola Storage API token. The value will be hidden.
? API token ****************************************************
Created metadata dir ".keboola".
Created manifest file ".keboola/manifest.json".
Created file ".env.local" - it contains the API token, keep it local and secret.
Created file ".env.dist" - an ".env.local" template.
Created file ".gitignore" - to keep ".env.local" local.
Init done. Running pull.
Plan for "pull" operation:
	+  B main
	+  C main/extractor/ex-generic-v2/729814342-my-generic-api
	+  C main/extractor/ex-generic-v2/729819493-some-api
	+  C main/extractor/keboola.ex-aws-s3/729820035-my-s-3-data
	+  R main/extractor/keboola.ex-aws-s3/729820035-my-s-3-data/rows/729820049-fin-stats
	+  C main/extractor/keboola.ex-google-drive/729819855-my-gdrive-sheets
	+  C main/other/keboola.variables/731079080-variables-definition-for-keboola-python-transformation-v-2-731078959
	+  R main/other/keboola.variables/731079080-variables-definition-for-keboola-python-transformation-v-2-731078959/rows/731079086-default-values
	+  C main/other/orchestrator/729819605-hourly-orchestration
	+  C main/transformation/keboola.oracle-transformation/731081310-test-oracle
	+  C main/transformation/keboola.python-transformation-v2/731078959-test-python
	+  C main/transformation/keboola.python-transformation-v2/731085760-test-local-created
	+  C main/transformation/keboola.snowflake-transformation/730131902-my-transformation
Pull done.
```
At this point it is good to init a git repository as well, so call `git init` and make an initial commit.
## Sync local directory changes into the remote project
The typical workflow here is to update an existing configuration locally and sync it to the remote project.
### kbc diff --details
Preview changes between remote project and local directory by calling `kbc diff --details`.
```
➜  my-kbc-project kbc diff --details
CH changed
-  remote state
+  local state

Diff:
CH C main/extractor/ex-generic-v2/729819493-some-api
	"configuration":
		"someProperty":
		+ someValue
```
### kbc push --dry-run
You can preview changes of the push process by calling `kbc push --dry-run`.

```
➜  my-kbc-project kbc push --dry-run
Plan for "push" operation:
	CH C main/extractor/ex-generic-v2/729819493-some-api | changed: configuration
Dry run, nothing changed.
Push done.
```
### kbc push
Finally, to sync local changes into the remote project call `kbc push`.
```
➜  my-kbc-project kbc push
Plan for "push" operation:
	CH C main/extractor/ex-generic-v2/729819493-some-api | changed: configuration
Push done.
```

## Create new configurations
Sometimes you need to create a new configuration from the local directory a sync it to remote project. In such you case you need to create files following the [outlined directory structure](README.md#directory-structure). After that use `kbc persist` command to prepare configuration(to automatically fill-up ids and additional info) and then call `kbc push` command. 
### Prepare configuration files
Let's say we want to create new `ex-generic-v2` confiuration named `wiki`. Under `main/extractor/ex-generic-v2/` we create a directory `wiki` where we create following files:
- `config.json` - with the following json content:
```
{
    "api": {
        "baseUrl": "https:wiki.com"        
    }
}
```
 - `meta.json` - with the following json content:
```
{
  "name": "extract wiki.com",
  "description": ""
}
```
 
The final file structure would look as follows:
```
 ➜  my-kbc-project tree
.
└── main
    ├── extractor
    │   ├── ex-generic-v2
    │   │   ├── 729814342-my-generic-api
    │   │   │   ├── config.json
    │   │   │   └── meta.json
    │   │   ├── 729819493-some-api
    │   │   │   ├── config.json
    │   │   │   └── meta.json
    │   │   └── wiki
    │   │       ├── config.json
    │   │       └── meta.json
```
### kbc persist
Call `kbc persist` command to finalize the wiki configuration files.
```
 ➜  my-kbc-project kbc persist
New objects:
	+ C 731114520 main/extractor/ex-generic-v2/wiki
Renamed objects:
	- main/extractor/ex-generic-v2/{wiki -> 731114520-extract-wiki-com}
Persist done.
```
Note that the `wiki` directory has been renamed `731114520-extract-wiki-com` to meet the naming conventions.
 
### kbc push --dry-run
Call `kbc push --dry-run` to preview changes:
```
➜  my-kbc-project kbc push --dry-run
Plan for "push" operation:
	+  C main/extractor/ex-generic-v2/731114520-extract-wiki-com
Dry run, nothing changed.
```
### kbc push
Finally, to sync local changes into the remote project call `kbc push`.
```
➜  my-kbc-project kbc push
Plan for "push" operation:
	+  C main/extractor/ex-generic-v2/731114520-extract-wiki-com
Push done.
``` 

## Sync remote changes to the local directory
Sometimes you create a new or change an existing configuration in the remote project.
### kbc diff --details
Preview changes between remote project and local directory call `kbc diff --details`.
```
➜  my-kbc-project kbc diff --details
CH changed
-  remote state
+  local state

Diff:
CH C main/extractor/ex-generic-v2/729819493-some-api
	"configuration":
		"newRemoteProperty":
		- newRemoteValue
```
### kbc pull --dry-run
You can preview the pull process first without actually change anything in the local directory by calling `kbc pull --dry-run` command.

```
➜  my-kbc-project kbc pull --dry-run
Plan for "pull" operation:
	CH C main/extractor/ex-generic-v2/729819493-some-api | changed: configuration
Dry run, nothing changed.
Pull done.
```
### kbc pull
To sync those changes into the local diretory call `kbc pull` command.
```
➜  my-kbc-project kbc pull
Plan for "pull" operation:
	CH C main/extractor/ex-generic-v2/729819493-some-api | changed: configuration
Pull done.
```
Note that calling `kbc pull` command will override any changes made to the local directory.


## FAQ
#### Does "kbc pull" command override local unpushed changes
Yes, if you made changes to the local files in the local directory without calling the `kbc push` command, the consequent call of `kbc pull` command will override those changes according to the state of the remote project.

