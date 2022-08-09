{
  storage: {
    output: {
      tables: [
        {
          source: "table",
          destination: "in.c-my-super-bucket-" + ConfigId("om-config") + ".table",
        },
      ],
    },
  },
  parameters: Input("oauthAccounts") + {
    api: {
      baseUrl: "https://jsonplaceholder.typicode.com",
    },
  },
  authorization: Input("oauth")
}
