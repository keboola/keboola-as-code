{
  parameters: {
    username: Input("shoptet-username"),
    password: Input("shoptet-password"),
    instanceIdLength: std.length(InstanceId()),
    instanceIdShortLength: std.length(InstanceIdShort()),
  }
}
