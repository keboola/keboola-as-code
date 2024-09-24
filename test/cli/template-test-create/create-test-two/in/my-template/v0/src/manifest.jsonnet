{
  configurations: [
    {
      componentId: "ex-generic-v2",
      id: ConfigId("empty"),
      path: "extractor/ex-generic-v2/empty",
      rows: [],
    },
    {
      componentId: "keboola.ex-db-mysql",
      id: ConfigId("with-rows"),
      path: "extractor/keboola.ex-db-mysql/with-rows",
      rows: [
        {
          id: ConfigRowId("users"),
          path: "rows/users",
        },
      ],
    },
  ],
}
