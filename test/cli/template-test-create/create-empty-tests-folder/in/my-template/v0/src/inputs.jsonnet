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
              description: "a",
              type: "string",
              kind: "input",
              default: "https://jsonplaceholder.typicode.com",
            },
            {
              id: "ex-db-mysql-db-host",
              name: "Db Host",
              description: "b",
              type: "string",
              kind: "input",
              default: "mysql.example.com",
            },
            {
              id: "ex-db-mysql-incremental",
              name: "Incremental",
              description: "c",
              type: "bool",
              kind: "confirm",
              default: false,
            },
            {
              id: "ex-db-mysql-db-password",
              name: "Db Pass",
              description: "b",
              type: "string",
              kind: "hidden",
            },
          ],
        },
      ],
    },
  ],
}
