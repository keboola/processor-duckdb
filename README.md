# DuckDB processor

DuckDB processor is a component that allows running SQL queries on DuckDB database. For more information about DuckDB, please visit the [DuckDB Docs website](https://duckdb.org/docs/).

## Configuration

Component supports two modes of operation, simple and advanced.
- **mode** - "simple" (default) / "advanced"


### Simple mode
In simple mode, each query operates on a single table in DuckDB which can be created from table defined by name, or by multiple tables matching pattern (along with an arbitrary number of files), and the query output is exported using the name of the input table.

In simple mode, you have the option to utilize the following parameters:
- **queries** - A list of queries to be executed each query contains:
  - **input** - string of name or pattern, or object containing following parameters:
    - **input_pattern** - (required) name of table or [glob pattern](https://duckdb.org/docs/data/multiple_files/overview#glob-syntax)
    - **duckdb_destination** - name of the table in DuckDB
    - **detect_dtypes** - boolean
    - **skip_lines** - number of lines to skip
    - **delimiter** - string of delimiter
    - **quotechar** - string of quotechar
    - **column_names** - list of column names
    - **date_format** - string of date format
    - **timestamp_format** - string of timestamp format
- **query** (required) query to be executed
- **output** - string of output table name or object containing following parameters:
  - **kbc_destination** - name of the output table
  - **primary_key** - list of primary keys
  - **incremental** - boolean

#### Example configuration

```
{
    "queries":[
      {
        "input": "sales",
        "query" : "SELECT sales_representative, SUM(turnover) AS total_turnover FROM sales GROUP BY sales_representative;",
      }]
}
```

```
{
    "mode": "simple",
    "queries":[
      {
        "input": {
          "input_pattern": "/data/in/files/*.csv",
          "duckdb_destination": "sales",
          "detect_dtypes": true,
          "skip_lines": 1,
          "delimiter": ",",
          "quotechar": "\"",
          "column_names": ["id", "name", "turnover", "sales_representative", "date"],
          "date_format": "YYYY-MM-DD",
          "timestamp_format": "YYYY-MM-DD HH:MM:SS"
        },
        "query" : "SELECT *, 'csv' as new_column FROM products AS p LEFT JOIN '/data/in/files/categories.parquet' AS cat on p.category = cat.id ORDER BY p.id",
        "output": {
          "kbc_destination": "out-sales",
          "primary_key": ["id"],
          "incremental": true
        }
      },{
        "input": "categories",
        "query" : "SELECT * FROM categories"
      }]
}
```


### Advanced mode
In advanced mode, first the [relations](https://duckdb.org/docs/api/python/relational_api) from specified input tables (or all input tables if not specified) are created.
Then all defined queries are processed.
Finally, output tables specified in out_tables are exported to storage.

parameters:
- **detect_types** - (Default: false) When set to true, data types for all columns will be inferred. If set to false, only new columns or columns without specified data types in the input manifest will be inferred.
- **in_tables** - (Optional) A list of input tables to be used in the queries. If not specified, all input tables are used.
- **queries** - A list of SQL queries to be executed.
- **out_tables** - A list of output tables to be exported. Each table can be specified as a string or a dictionary. If a dictionary is used, parameters are following:
  - **source** - (required) The name of the source table or view.
  - **destination** - The name of the destination table in storage.
  - **primary_keys** - A list of primary keys for the table.
  - **incremental** - (Default: false) When set to true, the table is exported incrementally.


#### Example configuration


```
{
    "mode": "advanced",
    "input": ["products"],
    "detect_types": "true",
    "queries":["CREATE view out AS SELECT * FROM products AS p LEFT JOIN '/data/in/files/categories.parquet' AS cat on p.category = cat.id ORDER BY p.id;",
               "CREATE view out2 AS SELECT * FROM products WHERE discount = TRUE;"],
    "output": ["out", {"source":"out2", "destination":"out.bucket.out2.csv", "primary_key": ["id"], "incremental":  true}],
}
```

Load file from url and save it as table
```
{
    "mode": "advanced",
    "queries":["CREATE view cars AS SELECT * FROM 'https://github.com/keboola/developers-docs/raw/3f1e8a4331638a2300b29e63f797a1d52d64929e/integrate/variables/countries.csv'"],
    "output": ["cars"]
}
```


# Development

This example contains runnable container with simple unittest. For local testing it is useful to include `data` folder
in the root and use docker-compose commands to run the container or execute tests.

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path:

```yaml
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
```

Clone this repository, init the workspace and run the component with following command:

```
git clone https://bitbucket.org:kds_consulting_team/kds-team.processor-rename-headers.git my-new-component
cd my-new-component
docker-compose build
docker-compose run --rm dev
```

Run the test suite and lint check using this command:

```
docker-compose run --rm test
```

# Testing

The preset pipeline scripts contain sections allowing pushing testing image into the ECR repository and automatic
testing in a dedicated project. These sections are by default commented out.

# Integration

For information about deployment and integration with KBC, please refer to
the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 