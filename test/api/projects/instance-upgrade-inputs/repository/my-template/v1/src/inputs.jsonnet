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
          ],
        },
      ],
    },
  ],
}
