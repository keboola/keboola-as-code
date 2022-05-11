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
              id: "input-1",
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
