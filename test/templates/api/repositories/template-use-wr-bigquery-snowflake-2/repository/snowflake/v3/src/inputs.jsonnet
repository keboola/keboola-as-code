{
  stepsGroups: [
    {
      description: 'Configure the Snowflake',
      required: 'all',
      steps: [
        {
          icon: 'component:' + SnowflakeWriterComponentId(),
          name: 'Snowflake',
          description: 'Destination',
          inputs: [
            {
              id: 'wr-snowflake-host',
              backend: "snowflake",
              name: 'Hostname',
              description: 'Insert database hostname',
              type: 'string',
              kind: 'input',
            },
          ],
        },
         {
           icon: 'component:keboola.wr-google-bigquery-v2',
           name: 'BigQuery',
           description: 'Destination',
           inputs: [
             {
               id: 'wr-bigquery-host',
               backend: "bigquery",
               name: 'Hostname',
               description: 'Insert database hostname',
               type: 'string',
               kind: 'input',
             },
           ],
         },
      ],
    },
  ],
}
