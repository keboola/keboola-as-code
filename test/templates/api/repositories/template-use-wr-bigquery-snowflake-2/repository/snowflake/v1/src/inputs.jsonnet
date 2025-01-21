{
  stepsGroups: [
    {
      description: 'Configure the Snowflake',
      required: 'all',
      steps: [
        {
          icon: 'component:keboola.wr-db-snowflake-gcs-s3',
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
      ],
    },
  ],
}
