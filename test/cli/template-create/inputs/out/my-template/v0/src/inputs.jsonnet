{
  stepsGroups: [
    {
      description: "Default Group",
      required: "all",
      steps: [
        {
          icon: "common:settings",
          name: "Default Step",
          description: "Default Step",
          inputs: [
            {
              id: "ex-generic-v2-api-base-url",
              name: "Api BaseUrl",
              description: "",
              type: "string",
              kind: "input",
              default: "https://jsonplaceholder.typicode.com",
            },
            {
              id: "ex-db-mysql-db-password",
              name: "Db Password",
              description: "",
              type: "string",
              kind: "hidden",
            },
            {
              id: "ex-db-mysql-db-host",
              name: "Db Host",
              description: "",
              type: "string",
              kind: "input",
              default: "mysql.example.com",
            },
            {
              id: "ex-db-mysql-db-port",
              name: "Db Port",
              description: "",
              type: "int",
              kind: "input",
              default: 3306,
            },
            {
              id: "ex-db-mysql-db-user",
              name: "Db User",
              description: "",
              type: "string",
              kind: "input",
              default: "root",
            },
            {
              id: "ex-db-mysql-incremental",
              name: "Incremental",
              description: "",
              type: "bool",
              kind: "confirm",
              default: false,
            },
          ],
        },
      ],
    },
  ],
}
