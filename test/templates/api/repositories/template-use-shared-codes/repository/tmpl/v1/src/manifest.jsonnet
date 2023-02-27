{
  configurations: [
    {
      componentId: "keboola.shared-code",
      id: ConfigId("shared-code"),
      path: "_shared/keboola.snowflake-transformation",
      rows: [
        {
          id: ConfigRowId("example"),
          path: "codes/example",
        },
      ],
    },
    {
      componentId: "keboola.snowflake-transformation",
      id: ConfigId("use-shared-codes"),
      path: "transformation/keboola.snowflake-transformation/use-shared-codes",
      rows: [],
    },
  ],
}
