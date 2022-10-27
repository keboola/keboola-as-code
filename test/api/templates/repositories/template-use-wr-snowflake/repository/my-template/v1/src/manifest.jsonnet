{
  configurations: [
    {
      componentId: SnowflakeWriterComponentId(),
      id: ConfigId("destination"),
      path: "writer/keboola.wr-snowflake/destination",
      rows: [
        {
          id: ConfigRowId("activity"),
          path: "rows/activity",
        },
      ]
    },
  ],
}
