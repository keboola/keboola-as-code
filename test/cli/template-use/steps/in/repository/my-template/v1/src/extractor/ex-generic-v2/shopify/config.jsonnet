{
  authorization: {
    oauth_api: Input("shopify-oauth")
  },
  parameters: {
    token: Input("shopify-token"),
    shop: Input("shopify-shop-name"),
  }
}
