{
  stepsGroups: [
    {
      description: 'Configure the Snowflake',
      required: 'all',
      steps: [
        // TODO: Has to be supported by backend
        // if HasProjectBackend('snowflake') == 'true' then
        {
          icon: 'component:' + SnowflakeWriterComponentId(),
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
        // else
        // {
        //   icon: 'component:keboola.wr-google-bigquery-v2',
        //   name: 'BigQuery',
        //   description: 'Destination',
        //   inputs: [
        //     {
        //       id: 'wr-bigquery-host',
        //       name: 'Hostname',
        //       description: 'Insert database hostname',
        //       type: 'string',
        //       kind: 'input',
        //     },
        //   ],
        // },
      ],
    },
  ],
}
