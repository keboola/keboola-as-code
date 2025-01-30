{
  configurations: [
    if HasProjectBackend('snowflake') then
      {
        componentId: SnowflakeWriterComponentId(),
        id: ConfigId('destination'),
        path: 'writer/keboola.wr-bigquery/destination',
        rows: [],
      },
  ],
}
