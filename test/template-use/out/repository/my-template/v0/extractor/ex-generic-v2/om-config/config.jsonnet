{
  storage: {
    output: {
      tables: [
        {
          source: "table",
          destination: "in.c-my-super-bucket-om-config.table",
        },
      ],
    },
  },
  parameters: {
    api: {
      baseUrl: "https://jsonplaceholder.typicode.com",
    },
  },
}
