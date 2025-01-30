{
  stepsGroups: [
    {
      description: 'Configure the BigQuery',
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
          },
          {
            icon: 'component:keboola.wr-google-bigquery-v2',
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
