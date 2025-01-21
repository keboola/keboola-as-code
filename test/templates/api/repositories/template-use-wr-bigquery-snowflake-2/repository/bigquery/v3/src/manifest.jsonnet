{
  configurations: [
    if HasProjectBackend('snowflake') then
      {
        componentId: SnowflakeWriterComponentId(),
        id: ConfigId('destination'),
        path: 'writer/keboola.wr-snowflake/destination',
        rows: [],
      }
    else
      {
        componentId: 'keboola.wr-google-bigquery-v2',
        id: ConfigId('destination'),
        path: 'writer/keboola.wr-bigquery/destination',
        rows: [],
      },
  ],
}
