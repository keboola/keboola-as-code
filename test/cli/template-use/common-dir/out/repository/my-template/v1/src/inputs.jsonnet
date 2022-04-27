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
          inputs: inputs,
        },
      ],
    },
  ],
}
