{
  stepsGroups: [
    {
      description: 'Configure the Database',
      required: 'all',
      steps: [
          {
            icon: 'component:<keboola.wr-snowflake>',
            name: 'Snowflake',
            description: 'Destination',
            inputs: [
              {
                id: 'wr-snowflake-host',
                name: 'Hostname',
                description: 'Insert database hostname',
                type: 'string',
                kind: 'input',
              },
            ],
          }
          {
            icon: 'component:<keboola.wr-snowflake>',
            name: 'BigQuery',
            description: 'Destination',
            inputs: [
              {
                id: 'wr-bigquery-host',
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
