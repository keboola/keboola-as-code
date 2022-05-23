{
  parameters: {},
  storage: {
    input: {
      tables: [
        {
          source: "in.c-my-super-bucket-" + ConfigId("om-config") + ".table",
          destination: "table",
        },
      ],
    },
  },
}
