{ mainConfig: {
    componentId: "keboola.orchestrator",
    id: ConfigId("flow-keboola-mailchimp"),
  },
  configurations: std.filter(function(v) v != null,[
    {
      componentId: "keboola.orchestrator",
      id: ConfigId("flow-keboola-mailchimp"),
      path: "other/keboola.orchestrator/flow-keboola-mailchimp",
      rows: [],
    },
    {
      componentId: "keboola.ex-mailchimp",
      id: ConfigId("data-source-1-in-mailchimp"),
      path: "<common>/in-mailchimp/extractor/keboola.ex-mailchimp/data-source-1-in-mailchimp",
      rows: [],
    },
    {
      componentId: "keboola.ex-mailchimp",
      id: ConfigId("data-source-2-in-mailchimp"),
      path: "<common>/in-mailchimp/extractor/keboola.ex-mailchimp/data-source-2-in-mailchimp",
      rows: [],
    },
    if HasProjectBackend("snowflake")==true then
    {
      componentId: "keboola.snowflake-transformation",
      id: ConfigId("transformation-in-mailchimp"),
      path: "<common>/in-mailchimp/transformation/keboola.snowflake-transformation/transformation-in-mailchimp",
      rows: [],
    },
    if HasProjectBackend("bigquery")==true then
    {
      componentId: "keboola.google-bigquery-transformation",
      id: ConfigId("transformation-in-mailchimp-bq"),
      path: "<common>/in-mailchimp/transformation/keboola.google-bigquery-transformation/transformation-in-mailchimp-bq",
      rows: [],
    },
    if InputIsAvailable("wr-google-bigquery-v2-service-account-private-key") then
    {
      componentId: "keboola.wr-google-bigquery-v2",
      id: ConfigId("data-destination-out-mailchimp-bigquery"),
      path: "<common>/out-mailchimp-bigquery/writer/keboola.wr-google-bigquery-v2/data-destination-out-mailchimp-bigquery",
      rows: [
        {
          id: ConfigRowId("campaign"),
          path: "rows/campaign",
        },
        {
          id: ConfigRowId("campaign-event"),
          path: "rows/campaign-event",
        },
        {
          id: ConfigRowId("customer"),
          path: "rows/customer",
        },
        {
          id: ConfigRowId("list"),
          path: "rows/list",
        },
        {
          id: ConfigRowId("list-member"),
          path: "rows/list-member",
        },
      ],
    },
    if InputIsAvailable("google-sheet-id") then
    {
      componentId: "keboola.wr-google-sheets",
      id: ConfigId("data-destination-out-mailchimp-gsheet"),
      path: "<common>/out-mailchimp-googlesheet/writer/keboola.wr-google-sheets/data-destination-out-mailchimp-gsheet",
      rows: [],
    },
    if InputIsAvailable("wr-snowflake-db-host") then
    {
      componentId: SnowflakeWriterComponentId(),
      id: ConfigId("data-destination-out-mailchimp-snowflake"),
      path: "<common>/out-mailchimp-snowflake/writer/keboola.wr-snowflake/data-destination-out-mailchimp-snowflake",
      rows: [
        {
          id: ConfigRowId("campaign-001"),
          path: "rows/campaign-001",
        },
        {
          id: ConfigRowId("campaign-event-001"),
          path: "rows/campaign-event-001",
        },
        {
          id: ConfigRowId("customer-001"),
          path: "rows/customer-001",
        },
        {
          id: ConfigRowId("list-001"),
          path: "rows/list-001",
        },
        {
          id: ConfigRowId("list-member-001"),
          path: "rows/list-member-001",
        },
      ],
    },
    if InputIsAvailable("wr-postgresql-db-hostname") then
    {
      componentId: "keboola.wr-db-pgsql",
      id: ConfigId("out-mailchimp-postgresql-writer"),
      path: "<common>/out-mailchimp-postgresql/v0/src/writer/keboola.wr-db-pgsql/out-mailchimp-postgresql-writer",
      rows: [
        {
          id: ConfigRowId("campaign"),
          path: "rows/campaign",
        },
        {
          id: ConfigRowId("campaign-event"),
          path: "rows/campaign-event",
        },
        {
          id: ConfigRowId("customer"),
          path: "rows/customer",
        },
        {
          id: ConfigRowId("list"),
          path: "rows/list",
        },
        {
          id: ConfigRowId("list-member"),
          path: "rows/list-member",
        },
      ],
    },
  ],)
}
