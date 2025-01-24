{
  stepsGroups: [
    {
      description: 'Configure the Database',
      required: 'all',
      steps: [
        {
          icon: 'component:' + SnowflakeWriterComponentId(),
          name: 'Snowflake',
          description: 'Destination',
          backend: 'snowflake',
          inputs: [
            {
              id: 'wr-snowflake-host',
              name: 'Hostname',
              description: 'Insert database hostname',
              type: 'string',
              kind: 'input'
            }
          ]
        },
        {
          icon: 'component:keboola.wr-google-bigquery-v2',
          name: 'BigQuery',
          backend: 'bigquery',
          description: 'Destination',
          inputs: [
            {
              id: 'wr-bigquery-host',
              name: 'Hostname',
              description: 'Insert database hostname',
              type: 'string',
              kind: 'input'
            }
          ]
        }
      ]
    }
  ]
}
