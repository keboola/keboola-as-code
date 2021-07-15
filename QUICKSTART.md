# Quickstart documentation
This document describes basic scenarious on how to use the CLI assuming the CLI has been already installed and ready to use.
### Init project folder
To start using CLI, a folder has to be initialized locally, the following prerequisities are needed:
- **Master API Token** - go to the remote project settings -> API Tokens and click on your token(labeled as `Yours`). In the token detail page click on `Refresh` button and refresh the token. Copy the refreshed Master API token to the clipboard. 
- **Empty project directory** - create an empty directory, name it whatever your like e.g. `mkdir my-kbc-project`

Call **`kbc init`** command from within the created empty project directory. The command asks for the host and Master API Token inputs. After the commands finishes the directory is initialized and contains all the configurations of the remote project, e.g.:

```shell
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
+  C main/other/orchestrator/729819605-hourly-orchestration
Pull done.
```
