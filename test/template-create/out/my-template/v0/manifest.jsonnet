{
  naming: {
    config: "{component_type}/{component_id}/{config_id}",
    configRow: "rows/{config_row_id}",
    schedulerConfig: "schedules/{config_id}",
    sharedCodeConfig: "_shared/{target_component_id}",
    sharedCodeConfigRow: "codes/{config_row_id}",
    variablesConfig: "variables",
    variablesValuesRow: "values/{config_row_id}",
  },
  configurations: [
    {
      componentId: "ex-generic-v2",
      id: "empty",
      path: "extractor/ex-generic-v2/empty",
      rows: [],
    },
    {
      componentId: "ex-generic-v2",
      id: "without-rows",
      path: "extractor/ex-generic-v2/without-rows",
      rows: [],
    },
    {
      componentId: "keboola.ex-aws-s3",
      id: "om-default-bucket",
      path: "extractor/keboola.ex-aws-s3/om-default-bucket",
      rows: [],
    },
    {
      componentId: "keboola.ex-db-mysql",
      id: "with-rows",
      path: "extractor/keboola.ex-db-mysql/with-rows",
      rows: [
        {
          id: "disabled",
          path: "rows/disabled",
        },
        {
          id: "test-view",
          path: "rows/test-view",
        },
        {
          id: "users",
          path: "rows/users",
        },
      ],
    },
    {
      componentId: "keboola.shared-code",
      id: "shared-codes",
      path: "other/keboola.shared-code/shared-codes",
      rows: [
        {
          id: "code-with-variables",
          path: "codes/code-with-variables",
        },
        {
          id: "my-code-1",
          path: "codes/my-code-1",
        },
        {
          id: "my-code-2",
          path: "codes/my-code-2",
        },
      ],
    },
    {
      componentId: "keboola.variables",
      id: "shared-code-variables",
      path: "variables",
      relations: [
        {
          configId: "shared-codes",
          rowId: "code-with-variables",
          type: "sharedCodeVariablesFor",
        },
      ],
      rows: [],
    },
    {
      componentId: "transformation",
      id: "old-transformation",
      path: "other/transformation/old-transformation",
      rows: [
        {
          id: "old-transformation-snfk",
          path: "rows/old-transformation-snfk",
        },
      ],
    },
    {
      componentId: "keboola.python-transformation-v2",
      id: "python-transformation",
      path: "transformation/keboola.python-transformation-v2/python-transformation",
      rows: [],
    },
    {
      componentId: "keboola.python-transformation-v2",
      id: "transformation-with-shared-code",
      path: "transformation/keboola.python-transformation-v2/transformation-with-shared-code",
      rows: [],
    },
    {
      componentId: "keboola.variables",
      id: "transformation-with-shared-code-variables",
      path: "variables",
      relations: [
        {
          componentId: "keboola.python-transformation-v2",
          configId: "transformation-with-shared-code",
          type: "variablesFor",
        },
      ],
      rows: [
        {
          id: "default-values",
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
      id: "snowflake-transformation",
      path: "transformation/keboola.snowflake-transformation/snowflake-transformation",
      rows: [],
    },
    {
      componentId: "keboola.wr-db-mysql",
      id: "im-default-bucket",
      path: "writer/keboola.wr-db-mysql/im-default-bucket",
      rows: [],
    },
  ],
}
