{
  configurations: [
    {
      componentId: "ex-generic-v2",
      id: ConfigId("shopify"),
      path: "extractor/ex-generic-v2/shopify",
      rows: [],
    },
    {
      componentId: "ex-generic-v2",
      id: ConfigId("shoptet"),
      path: "extractor/ex-generic-v2/shoptet",
      rows: [],
    },
    {
      componentId: "keboola.ex-aws-s3",
      id: ConfigId("om-default-bucket"),
      path: "extractor/keboola.ex-aws-s3/om-default-bucket",
      rows: [],
    },
    {
      componentId: "keboola.python-transformation-v2",
      id: ConfigId("python-transformation"),
      path: "transformation/keboola.python-transformation-v2/python-transformation",
      rows: [],
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
