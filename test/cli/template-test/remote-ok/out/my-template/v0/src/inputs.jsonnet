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
              id: "ex-generic-v2-api-base-token",
              name: "Api Token",
              description: "a",
              type: "string",
              kind: "hidden",
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
          ],
        },
      ],
    },
    {
      description: "Group 2",
      required: "all",
      steps: [
        {
          icon: "common:settings",
          name: "Step 2-1",
          description: "Step 2-1",
          inputs: [
            {
              id: "ex-generic-v2-api-base-url-2",
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
          name: "Step 2-2",
          description: "Step 2-2",
          inputs: [
            {
              id: "input-3",
              name: "A select",
              description: "a",
              type: "string",
              kind: "select",
              options: [
                {
                  value: "a",
                  label: "A",
                },
                {
                  value: "b",
                  label: "B"
                },
              ],
            },
          ],
        },
      ],
    },
  ],
}
