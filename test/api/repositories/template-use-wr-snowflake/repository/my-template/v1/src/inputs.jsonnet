{
  stepsGroups: [
    {
      description: "Configure the Snowflake",
      required: "all",
      steps: [
        {
          icon: "component:"+SnowflakeWriterComponentId(),
          name: "Snowflake",
          description: "Destination",
          inputs: [
             {
                id: "wr-snowflake-host",
                name: "Hostname",
                description: "Insert database hostname",
                type: "string",
                kind: "input",
              },
          ]
        },
      ],
    },
  ],
}
