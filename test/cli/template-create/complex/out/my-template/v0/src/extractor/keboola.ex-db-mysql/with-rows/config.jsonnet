{
  parameters: {
    db: {
      host: "mysql.example.com",
      port: 3306,
      user: "root",
      "#password": Input("ex-db-mysql-db-password"),
    },
  },
}
