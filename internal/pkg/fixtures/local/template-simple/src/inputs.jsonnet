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
              id: "generic-url",
              name: "Api BaseUrl",
              description: "url description",
              type: "string",
              kind: "input",
              default: "https://jsonplaceholder.typicode.com",
            },
            {
              id: "generic-token",
              name: "Api Token",
              description: "token description",
              type: "string",
              kind: "hidden",
            },
          ],
        },
      ],
    },
  ],
}
