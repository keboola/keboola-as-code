{
  authorization: {
    oauth_api: Input("oauth"),
  },
  parameters: Input("oauthAccounts") + {
    foo: "bar",
  }
}
