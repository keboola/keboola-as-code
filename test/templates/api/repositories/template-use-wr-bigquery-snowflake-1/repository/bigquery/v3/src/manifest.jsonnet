{
  configurations: [
    if HasProjectBackend('snowflake') then
      {
        componentId: SnowflakeWriterComponentId(),
        id: ConfigId('destination'),
        path: 'writer/keboola.wr-snowflake/destination',
        rows: [],
      },
    if HasProjectBackend('bigquery') then
      {
        componentId: SnowflakeWriterComponentId(),
        id: ConfigId('destination'),
        path: 'writer/keboola.wr-bigquery/destination',
        rows: [],
      },
  ],
}
