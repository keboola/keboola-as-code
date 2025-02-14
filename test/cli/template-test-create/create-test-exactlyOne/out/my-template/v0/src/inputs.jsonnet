{
  stepsGroups: [
    {
      description: "Default Group",
      required: "exactlyOne",
      steps: [
        {
          icon: "common:settings",
          name: "Default Step 1",
          description: "Default Step 1",
          inputs: [
            {
              id: "ex-generic-v2-api-base-url",
              name: "Api BaseUrl",
              description: "a",
              type: "string",
              kind: "input",
              default: "https://jsonplaceholder.typicode.com",
            },
          ],
        },
        {
          icon: "common:settings",
          name: "Default Step 2",
          description: "Default Step 2",
          inputs: [
            {
              id: "ex-db-mysql-db-host",
              name: "Db Host",
              description: "b",
              type: "string",
              kind: "input",
              default: "mysql.example.com",
            },
          ],
        },
        {
          icon: "common:settings",
          name: "Default Step 3",
          description: "Default Step 3",
          inputs: [
            {
              id: "ex-db-mysql-incremental",
              name: "Incremental",
              description: "c",
              type: "bool",
              kind: "confirm",
              default: false,
            },
          ],
        },
        {
          icon: "common:settings",
          name: "Default Step 4",
          description: "Default Step 4",
          inputs: [
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
