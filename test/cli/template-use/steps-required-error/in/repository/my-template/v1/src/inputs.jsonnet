{
  stepsGroups: [
    {
      description: "Choose one of the eshop platforms",
      required: "exactlyOne",
      steps: [
        {
          icon: "common",
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
              id: "shopify-shop-name",
              name: "Shop name",
              description: "Please enter Shop name",
              type: "string",
              kind: "input",
              rules: "required",
            },
          ],
        },
        {
          icon: "common",
          name: "Shoptet",
          description: "Sell online with Shoptet",
          inputs: [
            {
              id: "shoptet-username",
              name: "Shoptet username",
              description: "Please enter Shoptet username",
              type: "string",
              kind: "input",
              rules: "required",
            },
            {
              id: "shoptet-password",
              name: "Shoptet password",
              description: "Please enter Shoptet password",
              type: "string",
              kind: "hidden",
              rules: "required",
            },
          ],
        },
      ],
    },
    {
      description: "Configure transformation",
      required: "all",
      steps: [
        {
          icon: "common",
          name: "Python",
          description: "Description for Python parameters",
          inputs: [
            {
              id: "python-parameter",
              name: "Python Parameter",
              description: "Please enter Python Parameter",
              type: "string",
              kind: "input",
              rules: "required",
            },
          ],
        },
        {
          icon: "common",
          name: "Snowflake",
          description: "Description for Snowflake parameters",
          inputs: [
            {
              id: "snowflake-parameter",
              name: "Snowflake Parameter",
              description: "Please enter Snowflake Parameter",
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
