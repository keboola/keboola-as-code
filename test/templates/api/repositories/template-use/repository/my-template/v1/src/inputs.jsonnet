{
  stepsGroups: [
    {
      description: "Configure the eshop platforms",
      required: "all",
      steps: [
        {
          icon: "common:settings",
          name: "Shopify",
          description: "Sell online with an ecommerce website",
          inputs: [
            {
              id: "shopify-token",
              name: "Shopify token",
              description: "Please enter Shopify token",
              type: "string",
              kind: "hidden",
              rules: "required",
            },
            {
              id: "oauth",
              name: "Shopify oAuth",
              description: "Shopify Authorization",
              type: "object",
              kind: "oauth",
              componentId: "keboola.ex-shopify",
            },
            {
              id: "oauth2",
              name: "Instagram oAuth",
              description: "Instagram Authorization",
              type: "object",
              kind: "oauth",
              componentId: "keboola.ex-instagram",
            },
            {
              id: "oauth2Accounts",
              name: "Instagram Profiles",
              description: "Instagram Profiles",
              type: "object",
              kind: "oauthAccounts",
              oauthInputId: "oauth2",
            },
          ],
        },
        {
          icon: "common:settings",
          name: "Data App",
          description: "Visualize your data.",
          inputs: [
            {
              id: "data-app-param1",
              name: "Param 1",
              description: "Please enter param 1",
              type: "string",
              kind: "input",
              rules: "required",
            },
          ],
        },
      ],
    },
  ],
}
