{
  stepsGroups: [
    {
      description: "Configure the eshop platforms",
      required: "atLeastOne",
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
          icon: "common:settings",
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
  ],
}
