local data = if HasProjectBackend('snowflake') then
  {
    configurations: [
      {
        componentId: 'keboola.wr-google-bigquery-v2',
        id: ConfigId('destination'),
        path: 'writer/keboola.wr-bigquery/destination',
        rows: [],
      },
    ],
  };

data
