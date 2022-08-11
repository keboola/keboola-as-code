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
              id: "mysql-password",
              name: "MySQL password",
              description: "Please entry MySQL password",
              type: "string",
              kind: "hidden",
              rules: "required",
            },
            {
              id: "mysql-incremental",
              name: "Incremental Fetching",
              description: "Do you want to enable incremental fetching?",
              type: "bool",
              kind: "confirm",
              default: false,
            },
            {
              id: "oauth",
              name: "oAuth",
              description: "oAuth",
              type: "object",
              kind: "oauth",
              componentId: "keboola.ex-instagram",
              rules: "required",
            },
            {
              id: "oauthAccounts",
              name: "oAuth Accounts",
              description: "oAuth Accounts",
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
