{
  parameters: {
    db: {
      host: Input("ex-db-mysql-db-host"),
      port: Input("ex-db-mysql-db-port"),
      user: Input("ex-db-mysql-db-user"),
      "#password": Input("ex-db-mysql-db-password"),
    },
  },
}
