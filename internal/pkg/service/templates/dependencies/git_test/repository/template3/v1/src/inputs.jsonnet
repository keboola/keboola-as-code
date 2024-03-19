local inputs = import "/<common>/inputs.jsonnet";
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
              id: "base-url",
              name: "Base URL",
              description: "Base URL for Generic Extractor",
              type: "string",
              kind: "input",
              rules: "required,url",
            }
          ],
        },
      ],
    },
  ],
}
