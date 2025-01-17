{
  configurations: [
    if HasProjectBackend('snowflake') then
      {
        componentId: SnowflakeWriterComponentId(),
        id: ConfigId('destination'),
        path: 'writer/keboola.wr-snowflake/destination',
        rows: [],
      },
  ],
}
