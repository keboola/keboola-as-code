{
  parameters: {},
  storage: {
    input: {
      tables: [
        {
          columns: [],
          source: "in.c-keboola-ex-aws-s3-" + ConfigId("om-default-bucket") + ".table",
          destination: "table",
          where_column: "",
          where_operator: "eq",
          where_values: [],
        },
      ],
    },
  },
}
