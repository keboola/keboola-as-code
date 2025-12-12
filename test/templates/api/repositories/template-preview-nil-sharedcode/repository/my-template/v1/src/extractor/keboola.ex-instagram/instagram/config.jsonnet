{
  authorization: {
    oauth_api: Input("oauth2"),
  },
  parameters: Input("oauth2Accounts") + {
    foo: "bar",
  }
}
