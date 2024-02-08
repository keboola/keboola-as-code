package table

type DetailFlag struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
}

type DownloadFlags struct {
	StorageAPIHost string   `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	ChangeSince    string   `mapstructure:"changed-since" usage:"only export rows imported after this date"`
	ChangedUntil   string   `mapstructure:"changed-until" usage:"only export rows imported before this date"`
	Columns        []string `mapstructure:"columns" usage:"comma-separated list of columns to export"`
	Limit          uint     `mapstructure:"limit" usage:"limit the number of exported rows"`
	Where          string   `mapstructure:"where" usage:"filter columns by value"`
	Order          string   `mapstructure:"order" usage:"order by one or more columns"`
	Format         string   `mapstructure:"format" usage:"output format (json/csv)"`
	Timeout        string   `mapstructure:"timeout" usage:"how long to wait for the unload job to finish"`
	Output         string   `mapstructure:"output" shorthand:"o" usage:"path to the destination file or directory"`
	AllowSliced    bool     `mapstructure:"allow-sliced" usage:"output sliced files as a directory containing slices as individual files"`
}

type ImportFlags struct {
	StorageAPIHost     string   `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	Columns            string   `mapstructure:"columns" usage:"comma separated list of column names. If present, the first row in the CSV file is not treated as a header"`
	IncrementalLoad    bool     `mapstructure:"incremental-load" usage:"data are either added to existing data in the table or replace the existing data"`
	FileWithoutHeaders bool     `mapstructure:"file-without-headers" usage:"states if the CSV file contains headers on the first row or not"`
	PrimaryKeys        []string `mapstructure:"primary-key" usage:"primary key for the newly created table if the table doesn't exist"`
	FileDelimiter      string   `mapstructure:"file-delimiter" usage:"field delimiter used in the CSV file"`
	FileEnclosure      string   `mapstructure:"file-enclosure" usage:"field enclosure used in the CSV file"`
	FileEscapedBy      string   `mapstructure:"file-escaped-by" usage:"escape character used in the CSV file"`
}

type PreviewFlags struct {
	StorageAPIHost string   `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	ChangedSince   string   `mapstructure:"changed-since" usage:"only export rows imported after this date"`
	ChangedUntil   string   `mapstructure:"changed-until" usage:"only export rows imported before this date"`
	Columns        []string `mapstructure:"columns" usage:"comma-separated list of columns to export"`
	Limit          uint     `mapstructure:"limit" usage:"limit the number of exported rows"`
	Where          string   `mapstructure:"where" usage:"filter columns by value"`
	Order          string   `mapstructure:"order" usage:"order by one or more columns"`
	Format         string   `mapstructure:"format" usage:"output format (json/csv/pretty)"`
	Out            string   `mapstructure:"out" shorthand:"o" usage:"export table to a file"`
	Force          bool     `mapstructure:"force" usage:"overwrite the output file if it already exists"`
}

type UnloadFlags struct {
	StorageAPIHost string   `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	ChangedSince   string   `mapstructure:"changed-since" usage:"only export rows imported after this date"`
	ChangedUntil   string   `mapstructure:"changed-until" usage:"only export rows imported before this date"`
	Columns        []string `mapstructure:"columns" usage:"comma-separated list of columns to export"`
	Limit          uint     `mapstructure:"limit" usage:"limit the number of exported rows"`
	Where          string   `mapstructure:"where" usage:"filter columns by value"`
	Order          string   `mapstructure:"order" usage:"order by one or more columns"`
	Format         string   `mapstructure:"format" usage:"output format (json/csv)"`
	Async          bool     `mapstructure:"async" usage:"do not wait for unload to finish"`
	Timeout        string   `mapstructure:"timeout" usage:"how long to wait for job to finish"`
}

type UploadFlags struct {
	StorageAPIHost    string   `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	Columns           string   `mapstructure:"columns" usage:"comma separated list of column names. If present, the first row in the CSV file is not treated as a header"`
	IncrementalLoad   bool     `mapstructure:"incremental-load" usage:"data are either added to existing data in the table or replace the existing data"`
	FileWithoutHeader bool     `mapstructure:"file-without-headers" usage:"states if the CSV file contains headers on the first row or not"`
	PrimaryKeys       []string `mapstructure:"primary-key" usage:"primary key for the newly created table if the table doesn't exist"`
	FileName          string   `mapstructure:"file-name" usage:"name of the file to be created"`
	FileTags          string   `mapstructure:"file-tags" usage:"comma-separated list of file tags"`
	FileDelimiter     string   `mapstructure:"file-delimiter" usage:"field delimiter used in the CSV file"`
	FileEnclosure     string   `mapstructure:"file-enclosure" usage:"field enclosure used in the CSV file"`
	FileEscapedBy     string   `mapstructure:"file-escaped-by" usage:"escape character used in the CSV file"`
}
