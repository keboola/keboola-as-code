local data = if HasProjectBackend('bigquery') then
  {
    configurations: [
      {
        componentId: SnowflakeWriterComponentId(),
        id: ConfigId('destination'),
        path: 'writer/keboola.wr-snowflake/destination',
        rows: [],
      },
    ],
  };

data
