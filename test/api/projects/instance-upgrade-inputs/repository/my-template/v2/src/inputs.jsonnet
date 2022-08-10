{
  stepsGroups: [
    {
      description: "Configure...",
      required: "all",
      steps: [
        {
          icon: "common:settings",
          name: "Instagram",
          description: "Lorem ipsum",
          inputs: [
            {
              id: "limit",
              name: "Lime",
              description: "Some limit",
              type: "int",
              kind: "input",
              default: 2000,
              rules: "required",
            },
            {
              id: "size",
              name: "Size",
              description: "Some size",
              type: "int",
              kind: "input",
              rules: "required",
            },
            {
              id: "oauth",
              name: "Instagram oAuth",
              description: "Instagram Authorization",
              type: "object",
              kind: "oauth",
              componentId: "keboola.ex-instagram",
              rules: "required",
            },
            {
              id: "oauthAccounts",
              name: "Instagram Profiles",
              description: "Instagram Profiles",
              type: "object",
              kind: "oauthAccounts",
              oauthInputId: "oauth",
              rules: "required",
            },
          ],
        },
      ],
    },
  ],
}
