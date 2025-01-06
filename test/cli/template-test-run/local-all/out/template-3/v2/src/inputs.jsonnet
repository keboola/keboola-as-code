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
          ],
        },
      ],
    },
  ],
}
