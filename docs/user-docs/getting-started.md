# Getting Started

## Init the Directory

To manage a project using Keboola CLI, you need to initialize a directory. Create an empty directory, hop into it and run
the init command.

```
mkdir my-kbc-project
cd my-kbc-project
kbc init
```

The command runs interactively by default and asks for the URL of the Keboola instance you want to use and
a [Master token](https://help.keboola.com/management/project/tokens/#master-tokens) to your project. It pulls all 
configurations from the project to the local directory.

```
➜ kbc init

Please enter the Keboola Storage API host, e.g., "connection.keboola.com".
? API host connection.north-europe.azure.keboola.com

Please enter the Keboola Storage API token. Its value will be hidden.
? API token ***************************************************

Please select which project's branches you want to use with this CLI.
The other branches will still exist, but they will be invisible in the CLI.
? Allowed project's branches: only main branch

Created metadata directory ".keboola".
Created manifest file ".keboola/manifest.json".
Created file ".env.local" - it contains the API token, keep it local and secret.
Created file ".env.dist" - an ".env.local" template.
Created file ".gitignore" - to keep ".env.local" local.

? Generate workflows files for GitHub Actions? No

Init done. Running pull.
Plan for "pull" operation:
+ B main
+ C main/extractor/keboola.ex-aws-s3/my-aws-s-3-data-source
+ R main/extractor/keboola.ex-aws-s3/my-aws-s-3-data-source/rows/share-cities
+ C main/extractor/keboola.ex-google-drive/my-config
+ C main/extractor/keboola.ex-google-drive/my-config/schedules/scheduler-for-7241051
+ C main/other/keboola.orchestrator/daily
+ C main/other/keboola.orchestrator/daily/schedules/scheduler-for-7243915
+ C main/other/keboola.sandboxes/address
+ C main/transformation/keboola.snowflake-transformation/address
+ C main/transformation/keboola.snowflake-transformation/address/variables
+ R main/transformation/keboola.snowflake-transformation/address/variables/values/default
Pull done.
```

You could want to version your project in a git repository. Feel free to call `git init` and make an initial commit.
The init command can prepare workflows for GitHub Actions to keep your directory and the project in sync. 

## Push Changes to the Project

When you update your local directory, you can compare the changes with the project:

```
➜ kbc diff --details
* changed
- remote state
+ local state

Diff:
* C main/extractor/keboola.ex-google-drive/my-config | changed: configuration
```

Before pushing the changes to the project, you are able to preview them first:

```
➜ kbc push --dry-run
Plan for "push" operation:
  * C main/extractor/keboola.ex-google-drive/my-config | changed: configuration
Dry run, nothing changed.
Push done.
```

And finally, perform the actual changes to the project:

```
➜ kbc push
Plan for "push" operation:
  * C main/extractor/keboola.ex-google-drive/my-config | changed: configuration
Push done.
```

## Create New Configurations

The configuration can be created in two ways. You can create an empty configuration or copy an existing one.

### Create Empty Configuration
Let's say you want to download some data from Wikipedia. You can run [`kbc create`](commands/local/create/config.md) 
without options and be guided by an interactive dialog, or fill all the options:

```
➜ kbc create config -b main -c ex-generic-v2 -n wiki
Created new config "main/extractor/ex-generic-v2/wiki"
```

Edit file `main/extractor/ex-generic-v2/wiki/config.json` as 
a [Generic Extractor](https://help.keboola.com/components/extractors/generic-extractor/) configuration. A super basic 
configuration could look like this:

```json
{
  "api": {
    "baseUrl": "https://wikipedia.org"
  }
}
```

Now we can push it to the project:

```
➜ kbc push
Plan for "push" operation:
  + C main/extractor/ex-generic-v2/wiki
Push done.
```

### Create Configurations by Copy & Paste

Let's say you want to copy a configuration of your Generic Extractor. Duplicate its directory:

![Screenshot -- Duplicate a configuration directory](https://help.keboola.com/cli/getting-started/configurations-copy-1.jpg)

Run the `persist` command:

```
➜ kbc persist
Plan for "persist" operation:
  + C main/extractor/ex-generic-v2/wiki 2
Persist done.
Plan for "rename" operation:
  - main/extractor/ex-generic-v2/{wiki 2 -> wiki-001}
Rename done.
```

The directory name is fixed and the configuration added to the manifest:

![Screenshot -- Fixed configuration directory](https://help.keboola.com/cli/getting-started/configurations-copy-2.jpg)

## Pull Changes from the Project

When you create or change configurations in the project, you can pull them to the local directory.

Show the changes between the project and the local directory:

```
➜ kbc diff --details
* changed
- remote state
+ local state

Diff:
* C main/extractor/ex-generic-v2/wiki
  configuration:
    api.baseUrl:
      - https://en.wikipedia.org/wiki/Git
      + https://wikipedia.org
```

Preview the pull command without changing anything first:

```
➜ kbc pull --dry-run
Plan for "pull" operation:
  * C main/extractor/ex-generic-v2/wiki | changed: configuration
Dry run, nothing changed.
Pull done.
```

And finally, pull the changes to the local directory. Note that it will override any changes to your local directory:

```
➜ kbc pull
Plan for "pull" operation:
  * C main/extractor/ex-generic-v2/wiki | changed: configuration
Pull done.
```

## Next Steps

- [Directory Structure](structure.md)
- [Commands](commands.md)
