{
  stepsGroups: [
    {
      description: "Default Group",
      required: "all",
      steps: [
        {
          id: "step-1",
          icon: "common",
          name: "Default Step",
          description: "Default Step",
          inputs: [
            {
              id: "base-url",
              name: "Base URL",
              description: "Base URL for Generic Extractor",
              type: "string",
              kind: "input",
              rules: "required,url",
            },
          ],
        },
      ],
    },
  ],
}
