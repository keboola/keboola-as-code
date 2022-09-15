{
  stepsGroups: [
    {
      description: "Default Group",
      required: "all",
      steps: [
        {
          icon: "common:settings",
          name: "Default Step",
          description: "Default Step Description",
          inputs: [
            {
              id: "generic-url",
              name: "API URL",
              description: "url description",
              type: "string",
              kind: "input",
              default: "https://foo.bar",
            },
            {
              id: "generic-token",
              name: "API Token",
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
