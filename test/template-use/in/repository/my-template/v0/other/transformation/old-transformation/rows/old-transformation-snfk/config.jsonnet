{
  backend: "snowflake",
  disabled: false,
  id: "717513586",
  input: [],
  name: "old-transformation-snfk",
  output: [],
  packages: [],
  phase: 1,
  queries: [
    "CREATE TABLE \"test2\" AS SELECT $1, $2\nFROM VALUES( 1, 'two' ), ( 3, 'four' ), ( 5, 'six' );",
  ],
  requires: [],
  type: "simple",
}
