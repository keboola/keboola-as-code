{
  mainConfig: {
    componentId: "keboola.runner-config-test",
    id: ConfigId("empty"),
  },
  configurations: [
    {
      componentId: "keboola.runner-config-test",
      id: ConfigId("empty"),
      path: "application/keboola.runner-config-test/empty",
      rows: [],
    },
    {
      componentId: "keboola.runner-config-test",
      id: ConfigId("without-rows"),
      path: "application/keboola.runner-config-test/without-rows",
      rows: [],
    },
    {
      componentId: "keboola.ex-db-mysql",
      id: ConfigId("with-rows"),
      path: "extractor/keboola.ex-db-mysql/with-rows",
      rows: [
        {
          id: ConfigRowId("disabled"),
          path: "rows/disabled",
        },
        {
          id: ConfigRowId("test-view"),
          path: "rows/test-view",
        },
        {
          id: ConfigRowId("users"),
          path: "rows/users",
        },
      ],
    },
  ],
}
