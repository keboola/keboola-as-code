{
  configurations: [
    {
      componentId: "ex-generic-v2",
      id: ConfigId("empty"),
      path: "extractor/ex-generic-v2/empty",
      rows: [],
    },
    {
      componentId: "ex-generic-v2",
      id: ConfigId("without-rows"),
      path: "extractor/ex-generic-v2/without-rows",
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
