{
  configurations: [
    if HasProjectBackend('bigquery') == 'true' then
      {
        componentId: SnowflakeWriterComponentId(),
        id: ConfigId('destination'),
        path: 'writer/keboola.wr-snowflake/destination',
        rows: [],
      },
  ],
}
