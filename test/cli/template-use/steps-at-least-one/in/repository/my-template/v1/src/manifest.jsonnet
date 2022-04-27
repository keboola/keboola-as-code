{
  configurations: std.filter(function(v) v != null,[
    if InputIsAvailable("shopify-token") then {
      componentId: "ex-generic-v2",
      id: ConfigId("shopify"),
      path: "extractor/ex-generic-v2/shopify",
      rows: [],
    },
    if InputIsAvailable("shoptet-username") then {
      componentId: "ex-generic-v2",
      id: ConfigId("shoptet"),
      path: "extractor/ex-generic-v2/shoptet",
      rows: [],
    },
  ]),
}
