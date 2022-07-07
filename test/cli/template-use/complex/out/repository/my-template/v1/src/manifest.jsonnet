{
  mainConfig: {
    componentId: "keboola.orchestrator",
    id: ConfigId("orchestrator"),
  },
  configurations: [
    {
      componentId: "keboola.shared-code",
      id: ConfigId("shared-codes"),
      path: "_shared/keboola.python-transformation-v2",
      rows: [
        {
          id: ConfigRowId("code-with-variables"),
          path: "codes/code-with-variables",
        },
        {
          id: ConfigRowId("my-code-1"),
          path: "codes/my-code-1",
        },
        {
          id: ConfigRowId("my-code-2"),
          path: "codes/my-code-2",
        },
      ],
    },
    {
      componentId: "keboola.variables",
      id: ConfigId("shared-code-variables"),
      path: "variables",
      relations: [
        {
          configId: ConfigId("shared-codes"),
          rowId: ConfigRowId("code-with-variables"),
          type: "sharedCodeVariablesFor",
        },
      ],
      rows: [],
    },
    {
      componentId: "ex-generic-v2",
      id: ConfigId("empty"),
      path: "extractor/ex-generic-v2/empty",
      rows: [],
      metadata: {
        "KBC.test": "value"
      }
    },
    {
      componentId: "ex-generic-v2",
      id: ConfigId("om-config"),
      path: "extractor/ex-generic-v2/om-config",
      rows: [],
    },
    {
      componentId: "ex-generic-v2",
      id: ConfigId("without-rows"),
      path: "extractor/ex-generic-v2/without-rows",
      rows: [],
    },
    {
      componentId: "keboola.ex-aws-s3",
      id: ConfigId("om-default-bucket"),
      path: "extractor/keboola.ex-aws-s3/om-default-bucket",
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
    {
      componentId: "keboola.orchestrator",
      id: ConfigId("orchestrator"),
      path: "other/keboola.orchestrator/orchestrator",
      rows: [],
    },
    {
      componentId: "keboola.scheduler",
      id: ConfigId("scheduler"),
      path: "schedules/scheduler",
      relations: [
        {
          componentId: "keboola.orchestrator",
          configId: ConfigId("orchestrator"),
          type: "schedulerFor",
        },
      ],
      rows: [],
    },
    {
      componentId: "transformation",
      id: ConfigId("old-transformation"),
      path: "other/transformation/old-transformation",
      rows: [
        {
          id: ConfigRowId("old-transformation-snfk"),
          path: "rows/old-transformation-snfk",
        },
      ],
    },
    {
      componentId: "keboola.python-transformation-v2",
      id: ConfigId("python-transformation"),
      path: "transformation/keboola.python-transformation-v2/python-transformation",
      rows: [],
    },
    {
      componentId: "keboola.python-transformation-v2",
      id: ConfigId("transformation-with-shared-code"),
      path: "transformation/keboola.python-transformation-v2/transformation-with-shared-code",
      rows: [],
    },
    {
      componentId: "keboola.variables",
      id: ConfigId("transformation-with-shared-code-variables"),
      path: "variables",
      relations: [
        {
          componentId: "keboola.python-transformation-v2",
          configId: ConfigId("transformation-with-shared-code"),
          type: "variablesFor",
        },
      ],
      rows: [
        {
          id: ConfigRowId("default-values"),
          path: "values/default-values",
          relations: [
            {
              type: "variablesValuesFor",
            },
          ],
        },
      ],
    },
    {
      componentId: "keboola.snowflake-transformation",
      id: ConfigId("im-transformation"),
      path: "transformation/keboola.snowflake-transformation/im-transformation",
      rows: [
        {
          id: ConfigRowId("test"),
          path: "rows/test",
        },
      ],
    },
    {
      componentId: "keboola.snowflake-transformation",
      id: ConfigId("snowflake-transformation"),
      path: "transformation/keboola.snowflake-transformation/snowflake-transformation",
      rows: [],
    },
    {
      componentId: "keboola.wr-db-mysql",
      id: ConfigId("im-default-bucket"),
      path: "writer/keboola.wr-db-mysql/im-default-bucket",
      rows: [],
    },
  ],
}
