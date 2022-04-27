{
  parameters: {
    username: Input("shoptet-username"),
    password: Input("shoptet-password"),
    instanceIdLength: std.length(InstanceId()),
    instanceIdLShortLength: std.length(InstanceIdShort()),
  }
}
