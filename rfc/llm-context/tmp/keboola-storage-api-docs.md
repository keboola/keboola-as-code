FORMAT: 1A
HOST: https://connection.keboola.com

# Storage API
Storage API provides an interface for sending data to Keboola Connection (KBC). The following are the most
important features of the API:
- Importing CSV data into Table Storage
- Exporting CSV data from Table Storage
- Loading arbitrary files into File Storage
- Managing tables including keys and indexes
- Managing buckets including sharing
- Managing component configurations

The CSV files interchanged with Storage must adhere to the
[RFC4180 Specification](http://tools.ietf.org/html/rfc4180) and must use the `UTF-8` encoding.
The files can be sent uncompressed or gzipped. You can find more information about working
with these files in [our manual](https://help.keboola.com/storage/tables/csv-files/).

For a list of available clients and their features, see the
[Developers Documentation](https://developers.keboola.com/integrate/storage/).
In it you will also find more complex examples of
[working with the API](https://developers.keboola.com/integrate/storage/api/).
This documentation assumes that you are already familiar with the
[Keboola Connection Storage component](https://help.keboola.com/storage/).

## HTTP Response Codes
The response from Storage API will have an
[HTTP status code](https://en.wikipedia.org/wiki/List_of_HTTP_status_codes) that will help
you determine if the request was successful. In case of an error, the HTTP status code
will help you determine the cause of the error. The status codes used by Storage API are
listed in the below tables.

### Success responses

| Code | Description |
|------|-------------|
| 200 `OK` | The request was successful. |
| 201 `Created` | The request was successful and a new resource was created. |
| 202 `Accepted` | Used for Asynchronous tasks. A job resource is returned. The actual result must be obtained in another API call. |
| 204 `No Content` | The request was successful but there is nothing to return. Usually used as a response of DELETE requests. |

### Error responses

| Code | Description |
|------|-------------|
| 400 `Bad Request` | The request was invalid. Usually caused by invalid input data (missing arguments, invalid arguments values, etc.). The cause of the error is described in the response. |
| 401 `Unauthorized` | Authentication failed. |
| 403 `Forbidden` | You don't have access to the resource. |
| 404 `Not Found` | You're asking for something that doesn't exist. |
| 500 `Internal Server Error` | Something went wrong. We are sorry, it is our fault and we will make our best to fix it! Feel free to send us a ticket. |
| 503 `Temporary Unavailable` | This response is typically returned when the system is under maintenance. The maintenance reason and expected maintenance end time are also returned in the response. |

## Authentication
Authentication is done via Storage API **Token**. There are multiple options
for [obtaining a Storage API Token](https://help.keboola.com/storage/tokens/).
The token must be sent as a value of the `X-StorageApi-Token` HTTP header with each API call.

Most API calls work within a *KBC Project*. The project is always derived from the token used for the API call.
So the token includes both authentication and authorization to a single project.

## Synchronous and Asynchronous Calls
Calls that represent potentially long-running actions are performed asynchronously.
These are for example: loading table data, snapshotting, exporting table data, table structure modifications, etc.
All asynchronous calls return the HTTP Response code `202` and a Job Resource.
To obtain the actual result of the call, you have to monitor the job status by
polling the Job Resource URL.

## Partial Response
Some API calls that list objects are returning related resources by default. For example,
the table list also returns the table bucket properties and table attributes. You can use the `include` parameter
to return only the required resources in the response.

## HTTP headers
Some API calls supports custom http headers.

Supported HTTP headers are:
- `X-StorageApi-Token (string)`: storage api authentication token
- `X-KBC-RunId (string)`: id mainly used to match component run with storage job
- `X-KBC-Backend (json string)`: header used to customize underlying backend configuration. Supported options are `context` and `size` (Supported option are different for each backend). Example: `'{"context":"custom-wlm-group","size":"XL"}'`

---

# Tokens and Permissions
When joining a project, each project administrator is assigned a master token,
which enables them to create other tokens with limited privileges for buckets and components.

Selected of tokens endpoints can be used with `default` for `branch_id` parameter and emulate call endpoint without development branch.
It means that endpoint `GET /v2/storage/tokens/verify` is the same as `GET /v2/storage/branch/default/tokens/verify`.

## Tokens Collection

### List all tokens
`GET /v2/storage/tokens`

Lists all tokens in the project.

**Request Headers:**
- `X-StorageApi-Token: your_token`

**Response 200 (application/json):**
Returns array of token objects with properties like:
- `id`, `token`, `description`
- `created`, `refreshed`, `uri`
- `isMasterToken`, `canManageBuckets`, `canManageTokens`
- `canReadAllFileUploads`, `canPurgeTrash`
- `expires`, `isExpired`, `isDisabled`
- `bucketPermissions`, `componentAccess`
- `admin` (for master tokens)

### List all tokens in Development branch
`GET /v2/storage/branch/{branch_id}/tokens`

Lists all tokens in the project - same as for default branch.

### Create token
`POST /v2/storage/tokens`

Creates a new token in the project. Note that it is not possible to create a new master token.

If your admin token does not have `canManageTokens` permission, you can only create temporary token with minimal privileges.
In this case, you are allowed to set the `description` and token expiration via `expiresIn` parameter. All other attributes will be ignored.

**Request Parameters:**
- `description` (optional) - Token description
- `bucketPermissions[{id_bucket}]` (optional) - Permissions for a bucket. Available permissions: `read`, `write`.
- `componentAccess[]` (optional) - Grants access for component configurations.
- `canManageBuckets` (optional, boolean) - Allows full access to all buckets including newly created buckets. Default: false
- `canReadAllFileUploads` (optional, boolean) - Allows access to all file uploads. Default: false
- `canPurgeTrash` (optional, boolean) - Allows permanently remove deleted configurations. Default: false
- `expiresIn` (optional, number) - Number of seconds until the token expires.
- `canManageProtectedDefaultBranch` (optional, boolean) - EXPERIMENTAL
- `canCreateJobs` (optional, boolean) - EXPERIMENTAL

## Token Detail

### Token detail
`GET /v2/storage/tokens/{id_token}`

Returns all properties of a token.

### Development branch token detail
`GET /v2/storage/branch/{branch_id}/tokens/{token_id}`

Returns all properties of a token - same as for default branch.

### Update Token
`PUT /v2/storage/tokens/{id_token}`

Updates Token properties. Beware that all current bucket permissions are replaced.

**Request Parameters:**
- `description` (required) - Token description
- `bucketPermissions[{id_bucket}]` (optional) - Permissions for the bucket. Available permissions: `read`, `write`.
- `componentAccess[]` (optional) - Grants access for component configurations.
- `canReadAllFileUploads` (optional, boolean) - Default: false
- `canPurgeTrash` (optional, boolean) - Default: false

### Delete Token
`DELETE /v2/storage/tokens/{id_token}`

It is possible to delete only the token that is not the master token.

## Token Verification

### Token verification
`GET /v2/storage/tokens/verify`

Checks the token privileges and returns information about the project to which the token belongs (`owner`)
and the associated administrator (`admin`) with organization (if any). This call can be executed by all tokens.

**Response includes:**
- Token information (id, description, permissions)
- Admin information (id, name, email, role)
- Organization information (if applicable)
- Owner (project) information including:
  - Project details (id, name, type, region)
  - Features and backends (snowflake, redshift, bigquery, etc.)
  - Limits (jobsParallelism, dataSizeBytes, etc.)
  - Metrics (current usage)

### Development branch token verification
`GET /v2/storage/branch/{branch_id}/tokens/verify`

Same as for default branch.

## Share Token
`POST /v2/storage/tokens/{token_id}/share`

Use this call whenever you want to securely deliver a token to someone. The link to the token retrieval page will be sent to the
provided email address. The link will expire in 2 hours.

**Request Parameters:**
- `recipientEmail` (required) - Recipient's email address
- `message` (required) - Message for the token recipient

## Token Refresh
`POST /v2/storage/tokens/{id_token}/refresh`

Use this method to generate a new token value; the old token value will become immediately invalid.
This method can be executed by all tokens.

---

# Miscellaneous

## API Index

### Component List
`GET /v2/storage?exclude={exclude}`

Use this API call to obtain definitions of all components, services and stack features available in KBC.

**Parameters:**
- `exclude` (optional) - Comma-separated list of resources or parts to exclude from response
  - `components` - the `components` field won't be present in a response
  - `componentDetails` - the `components` field will include only basic properties

**Response includes:**
- `api`, `documentation`, `version`
- `components[]` - List of available components
- `services[]` - List of available services
- `features[]` - Stack features
- `urlTemplates` - URL templates for various resources

---

# Buckets
Buckets are containers for one or more data tables.
Access to buckets can be limited by access tokens. Each bucket has a backend in which all tables are created:
- Snowflake (default)
- Redshift
- BigQuery

## Create or List Buckets

### List all buckets
`GET /v2/storage/buckets?include={include}`

All buckets are returned.

**Parameters:**
- `include` (optional) - Comma-separated list of resources to include for each table.
  - `metadata`
  - `linkedBuckets`

**Response includes bucket properties:**
- `id`, `name`, `displayName`, `stage`, `description`
- `uri`, `tables` (URL)
- `backend` (snowflake, redshift, bigquery)
- `linkedBy[]` - Projects that linked this bucket
- `sourceBucket` - For linked buckets
- `isReadonly` - For linked buckets
- `owner` - Bucket owner information
- `metadata[]` - Bucket metadata
- `color` - Bucket color

### Create Bucket
`POST /v2/storage/buckets`

Using this API call, you can either create a new bucket in the project, or you can link an
existing bucket from another project.

**Request Parameters:**
- `name` (required) - New bucket name; only alphanumeric characters and underscores are allowed.
- `stage` (optional) - Assigns the bucket to one of the stages: `in`, `out`. Default: `in`
- `description` (optional) - Bucket description. **deprecated**
- `backend` (optional) - Bucket backend type: `snowflake`, `redshift`, `bigquery`
- `displayName` (optional) - Bucket displayName, this name is displayed in UI
- `color` (optional) - Bucket color. Accept valid CSS values for colors.

**Response 201:** Returns created bucket object

### Link Shared Bucket
`POST /v2/storage/buckets?async={async}`

Use this API call to create a new bucket which contains contents of a
shared bucket in a source project.
Linking a bucket from another project is only possible if it has been
enabled in the source project.

**Parameters:**
- `async` (optional, boolean) - Share the bucket in a background job.

**Request Parameters:**
- `name` (required) - New bucket name
- `sourceProjectId` (required, number) - Id of the source project from which the bucket is being shared
- `sourceBucketId` (required) - Id of the bucket being shared
- `stage` (optional) - `in` or `out`. Default: `in`

**Response 201:** Synchronous call returns created bucket object
**Response 202:** Asynchronous call returns job object

### Register bucket
`POST /v2/storage/buckets/register`

**EXPERIMENTAL**

Use this API call to create a new bucket from existing resource (schema or DB - depends on backend). Available for **Snowflake** and **BigQuery** backends only

**Request Parameters:**
- `name` (required) - New bucket name
- `stage` (optional) - `in` or `out`. Default: `in`
- `description` (optional) - Bucket description
- `backend` (optional) - `snowflake` or `bigquery`
- `displayName` (optional) - Bucket displayName
- `path` (required, array) - Path to the schema. On SNFLK ['database-name','schema-name'], BigQuery ['projectId', 'location', 'exchangerName', 'listingName']
- `isSnowflakeSharedDatabase` (optional, boolean) - Flag if external bucket uses Snowflake Secure Data Share feature

**Response 202:** Asynchronous call returns job object

### Register bucket - guide
`GET /v2/storage/buckets/register-guide?backend={backend}&path={path}`

**EXPERIMENTAL**

Returns a guide on how exactly you should register the external bucket. The text is Markdown formatted. Available for **Snowflake** and **BigQuery** backend only

**Parameters:**
- `backend` (optional) - `snowflake` or `bigquery`
- `path` (required, array[string]) - Path to the schema
- `isSnowflakeSharedDatabase` (optional, boolean) - render guide for Snowflake shared database

## Manage Bucket

### Bucket Detail
`GET /v2/storage/buckets/{bucket_id}?include={include}`

Obtains information about a bucket.

**Parameters:**
- `bucket_id` (required) - Bucket Id
- `include` (optional) - Comma-separated list of resources:
  - `metadata` - each table will include list of its metadata
  - `columns` - each table will include list of its columns
  - `columnMetadata` - each table will include list of its columns metadata

### Bucket Update
`PUT /v2/storage/buckets/{bucket_id}/?async={async}`

Update an existing bucket

**Parameters:**
- `bucket_id` (required) - Bucket Id
- `async` (optional, boolean) - Updates the bucket in a background job

**Request Parameters:**
- `displayName` (required, string) - Bucket displayName
- `color` (optional) - Bucket color

**Response 200:** Synchronous call returns updated bucket object
**Response 202:** Asynchronous call returns job object

### Bucket Refresh
`PUT /v2/storage/buckets/{bucket_id}/refresh`

**EXPERIMENTAL**

Refresh external bucket. For **Snowflake** and **Bigquery** backends only.

**Response 202:** Returns job object

### Schedule bucket refresh
`POST /v2/storage/branch/{branch_id}/buckets/{bucket_id}/scheduled-tasks/refresh`

**EXPERIMENTAL**

Schedule external bucket refresh to given time or repeating period defined by cron expression.

**Request Parameters:**
- `cronExpression` (required) - Cron expression

**Response 201:** Returns scheduled task data

### Bucket Tables Information Refresh
`POST /v2/storage/branch/{branch_id}/buckets/{bucket_id}/refresh-tables-info`

Refresh tables information in bucket. For **Snowflake** backend only.

**Response 202:** Returns job object

### Drop Bucket
`DELETE /v2/storage/buckets/{bucket_id}/?force={force}&async={async}`

Deletes a bucket from the project. In the default mode, only empty buckets can be deleted.
Use the optional `force` parameter to delete all bucket content too.

**Parameters:**
- `bucket_id` (required) - Bucket Id
- `force` (optional, boolean) - Drops all tables and aliases
- `async` (optional, boolean) - Drops the bucket in a background job

**Response 204:** Synchronous call - bucket deleted
**Response 202:** Asynchronous call returns job object

### Bucket Owner detail
`GET /v2/storage/buckets/{bucket_id}/owner`

Return information about current bucket owner.

### Bucket Owner update
`POST /v2/storage/buckets/{bucket_id}/owner`

Update information about current bucket owner.

**Parameters:**
- `id` (optional, boolean) - Project Admin ID. ID or email is required.
- `email` (optional, boolean) - Project Admin email. ID or email is required.

## Bucket Sharing

### Share Bucket To Organization
`POST /v2/storage/buckets/{bucket_id}/share-organization`

Enables sharing or update sharing for this bucket within the organization.
Any organization member will be able to link to this bucket from any project within the organization.
This operations is available only for organization administrators.

**Response 202:** Returns job object

### Share Bucket To Organization Project
`POST /v2/storage/buckets/{bucket_id}/share-organization-project`

Enables sharing or update sharing for this bucket within the organization.
Any member of a project within the organization will be able to link to this bucket from their project.
This operations is available only for organization administrators.

**Response 202:** Returns job object

### Share Bucket To Projects
`POST /v2/storage/buckets/{bucket_id}/share-to-projects`

Enables sharing or update sharing for this bucket to a specific set of projects in the organization.
This operations is available only for organization administrators.

**Request Parameters:**
- `targetProjectIds[]` (required, array) - array of projects id

**Response 202:** Returns job object

### Share Bucket To Users
`POST /v2/storage/buckets/{bucket_id}/share-to-users`

Enables sharing or update sharing for this bucket to a specific set of users in the organization.
This operations is available only for organization administrators.

**Request Parameters:**
- `targetUsers[]` (required, array) - array of users id or user emails

**Response 202:** Returns job object

### Change Bucket Sharing
`PUT /v2/storage/buckets/{bucket_id}/share`

Change sharing type of a bucket.
This operations is available only for organization administrators.

**Request Parameters:**
- `sharing` (required) - Sharing type:
  - `organization` - Bucket is shared to organization
  - `organization-project` - Bucket is shared to organization project

**Response 202:** Returns job object

### Stop Bucket Sharing
`DELETE /v2/storage/buckets/{bucket_id}/share`

Disables sharing of a bucket. A bucket must not be linked to other projects.
This operations is available only for organization administrators.

**Response 202:** Returns job object

### Force unlink a bucket from a project
`DELETE /v2/storage/buckets/{bucket_id}/links/{linkedProjectId}?async={async}`

Force unlink allows you to unlink a bucket from any project it's linked to.

**Parameters:**
- `bucket_id` (required) - Bucket Id
- `linkedProjectId` (required, number) - id of project where the bucket is linked
- `async` (optional, boolean) - Unlinks the bucket in a background job

**Response 204:** Synchronous call - bucket unlinked
**Response 202:** Asynchronous call returns job object

## List Shared Buckets

### Shared buckets list
`GET /v2/storage/shared-buckets?include={include}`

Lists buckets which may be shared to the project.

**Parameters:**
- `include` (optional) - Comma-separated list of resources: `metadata`

**Response includes:**
- Bucket information
- `sharing` type
- `project` information (source project)
- `tables[]` - List of tables in bucket
- `sharedBy` - Information about who shared the bucket

---

# Tables

## Create or List Tables

### Create new table from CSV file
`POST /v2/storage/buckets/{bucket_id}/tables`

**DEPRECATED**, use Create table asynchronously instead.

Creates a new table in a bucket. The CSV file must follow the RFC 4180 Specification.

**Parameters:**
- `bucket_id` (required) - Bucket Id

**Request Parameters:**
- `name` (required) - New table name
- `data` (required) - CSV file containing data for import (UTF-8 encoded)
- `delimiter` (optional) - Field delimiter. Default: ','
- `enclosure` (optional) - Field enclosure. Default: '"'
- `escapedBy` (optional) - Escape character. Default: empty
- `primaryKey` (optional) - Primary key of the table

**Response 201:** Returns created table object

### Tables in bucket
`GET /v2/storage/buckets/{bucket_id}/tables?include={include}`

Lists tables in a given bucket.

**Parameters:**
- `bucket_id` (required) - Bucket Id
- `include` (optional) - Comma-separated list: `columns`, `metadata`, `columnMetadata`. Default: `metadata`

**Response includes table properties:**
- `id`, `name`, `displayName`
- `primaryKey[]`, `columns[]`
- `created`, `lastImportDate`, `lastChangeDate`
- `rowsCount`, `dataSizeBytes`
- `isAlias`, `isAliasable`, `isTyped`
- `timestampType`, `tableType`
- `metadata[]`, `columnMetadata{}`
- `sourceTable` (for aliases)
- `bucket` information

## Create Table Definition

### Create new table definition
`POST /v2/storage/buckets/{bucket_id}/tables-definition`

Creates a table with typed columns. *Snowflake|BigQuery only.*

Request is handled asynchronously.

**Parameters:**
- `bucket_id` (required) - The bucket Id

**Request Parameters:**
- `name` (required) - New table name
- `primaryKeysNames[]` (string) - Has to be subset of column names and not nullable
- `timestampType` (optional) - `MANAGED` or `NONE`. Default: `MANAGED`
- `columns[]` (required) - definition of table columns
  - `name` (required, string) - column name
  - `definition` (object) - column settings
    - `type` (string) - datatype of this column
    - `length` (string) - optional
    - `nullable` (boolean) - optional, default: true
    - `default` (string) - optional default value
  - `basetype` (enum[string]) - base datatype: `BOOLEAN`, `DATE`, `FLOAT`, `INTEGER`, `NUMERIC`, `STRING`, `TIMESTAMP`
- `timePartitioning` - BigQuery only
  - `type` (required) - `DAY`, `HOUR`, `MONTH`, `YEAR`
  - `field` (string)
  - `expirationMs` (number)
- `clustering` - BigQuery only
  - `fields[]` (required)
- `rangePartitioning` - BigQuery only
  - `field` (required, string)
  - `range` (required)
    - `start` (required, number)
    - `end` (required, number)
    - `interval` (required, number)

**Response 202:** Returns job object

## Update Definition

### Update Column Definition
`PUT /v2/storage/tables/{table_id}/columns/{column_name}/definition`

Updates a column of a table with typed columns. *Snowflake, BigQuery only.*

**Parameters:**
- `table_id` (required) - The table Id
- `column_name` (required) - The column name

**Request Parameters:**
- `length` (string) - optional
- `nullable` (boolean) - optional
- `default` (string) - optional, null unsets default value

**Supported alterations:**

| Operation | Snowflake | BigQuery |
|-----------|-----------|----------|
| Add default | ❌ | ✅ |
| Change default | ❌ | ✅ |
| Drop default | ✅ | ✅ |
| Set as nullable | ✅ | ✅ |
| Set as required | ✅ | ❌ |
| Change type | ❌ | ❌ |
| Increase text length | ✅ | ✅ |
| Decrease text length | ❌ | ❌ |
| Increase numeric precision | ✅ | ✅ |
| Decrease numeric precision | ❌ | ❌ |
| Change numeric scale | ❌ | ✅ |

**Response 202:** Returns job object

## Pull table from default branch to dev branch

### Pull table from default branch to dev branch
`POST /v2/storage/branch/{branch_id}/tables/{table_id}/pull`

**EXPERIMENTAL**

Pull table from default branch to dev branch with all data and metadata, if table already exists in dev branch, it will be overwritten.

**Parameters:**
- `table_id` (required) - The table string Id
- `branch_id` - Id of development branch

**Response 202:** Returns job object

## Create Table Asynchronously

### Create new table from CSV file asynchronously
`POST /v2/storage/buckets/{bucket_id}/tables-async`

The recommended way to create tables. Request is handled asynchronously.

**Parameters:**
- `bucket_id` (required) - Bucket Id

**Request Parameters:**
- Same as synchronous create table

**Response 202:** Returns job object

---

# Additional Information

For complete and up-to-date documentation including:
- Table operations (import, export, snapshots)
- Jobs management
- File uploads
- Metadata
- Workspaces
- Development branches
- Events and logs

Please visit: https://keboola.docs.apiary.io/

## Useful Links
- **API Documentation**: https://keboola.docs.apiary.io/
- **Developers Documentation**: https://developers.keboola.com/
- **User Documentation**: https://help.keboola.com/
- **Storage API GitHub**: https://github.com/keboola/storage-api-php-client
