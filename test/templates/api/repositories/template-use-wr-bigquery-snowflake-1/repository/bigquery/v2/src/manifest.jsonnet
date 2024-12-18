{
  configurations: [
    if HasProjectBackend('snowflake') == 'true' then
      {
        componentId: 'keboola.wr-google-bigquery-v2',
        id: ConfigId('destination'),
        path: 'writer/keboola.wr-bigquery/destination',
        rows: [],
      },
  ],
}
