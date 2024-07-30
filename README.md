# DuckDB Processor

The DuckDB processor is a component that allows running SQL queries on the DuckDB database. For more information about DuckDB, please visit the [DuckDB Docs website](https://duckdb.org/docs/).

## Configuration

The component supports two **modes of operation**:
- Simple (default)
- Advanced

### Simple Mode
In simple mode, each query operates on a single table in DuckDB that can be created from a table defined by name, or from multiple tables matching a pattern (along with an arbitrary number of files). 
The query output is exported using the name of the input table.

In simple mode, the parameter **queries** is an array of queries to be executed. Each query has own input, query, and output, and is isolated.
Each query can use the following parameters:

- **input:** A string specifying the name or pattern, or an object containing the following parameters:
  - **input_pattern** (required): The name of the table or [glob pattern](https://duckdb.org/docs/data/multiple_files/overview#glob-syntax)
  - **duckdb_destination:** The name of the table in DuckDB
  - **dtypes_mode:** all_varchar (default) / auto_detect / from_manifest
    - all_varchar: Treats all columns as text
    - auto_detect: Automatically infers data types
    - from_manifest: Uses data types from the input manifest (if a wildcard is used, the manifest of the first table is used)
  - **skip_lines:** The number of lines to skip
  - **delimiter:** A string specifying the delimiter
  - **quotechar:** A string specifying the quote character
  - **column_names:** A list of column names
  - **date_format:** A string specifying the date format
  - **timestamp_format:** A string specifying the timestamp format
- **query** (required): The query to be executed
- **output:** A string specifying the output table name or an object containing the following parameters:
  - **kbc_destination:** The name of the output table
  - **primary_key:** A list of primary keys
  - **incremental:** A boolean value indicating whether the data should be loaded incrementally

#### Example configuration

```
{
    "queries":[
      {
        "input": "sales.csv",
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
          "dtypes_mode": "auto_detect",
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


### Advanced Mode
In advanced mode, the [relations](https://duckdb.org/docs/api/python/relational_api) from all specified input tables are created first.
Then, all defined queries are processed. Finally, output tables specified in out_tables are exported to Keboola Storage.

Parameters:
 - **input:** An array of all input tables from Keboola, defined either as a string containing the name/pattern, or as an object containing the following parameters:
    - **input_pattern** (required): The name of the table or [glob pattern](https://duckdb.org/docs/data/multiple_files/overview#glob-syntax)
    - **duckdb_destination:** The name of the table in DuckDB
    - **dtypes_mode:** all_varchar (default) / auto_detect / from_manifest 
      - all_varchar: Treats all columns as text
      - auto_detect: Automatically infers data types
      - from_manifest: Uses data types from the input manifest (if a wildcard is used, the manifest of the first table is used)
    - **skip_lines:** The number of lines to skip
    - **delimiter:** A string specifying the delimiter
    - **quotechar:** A string specifying the quote character
    - **column_names:** A list of column names
    - **date_format:** A string specifying the date format
    - **timestamp_format:** A string specifying the timestamp format
- **queries:** A list of SQL queries to be executed
- **output:** An array of all output tables, defined as a string specifying the output table name or as an object containing the following parameters:
  - **kbc_destination:** The name of the output table
  - **primary_key:** A list of primary keys
  - **incremental:** A boolean value indicating whether the data should be loaded incrementally

#### Example configuration


```
{
    "mode": "advanced",
    "input": ["sliced", "days.csv"],
    "queries":["CREATE view final AS SELECT * FROM sliced LEFT JOIN days.csv USING (id) ORDER BY id"],
    "output": [
                {"duckdb_source": "final",
                 "kbc_destination": "final.csv"
                 }
               ]
}
```

```
{
    "mode": "advanced",
    "input": ["products"],
    "queries":["CREATE view out AS SELECT * FROM products AS p LEFT JOIN '/data/in/files/categories.parquet' AS cat on p.category = cat.id ORDER BY p.id;",
               "CREATE view out2 AS SELECT * FROM products WHERE discount = TRUE;"],
    "output": ["out", {"source":"out2", "destination":"out.bucket.out2.csv", "primary_key": ["id"], "incremental":  true}],
}
```

Load file from the URL and save it as a table
```
{
    "mode": "advanced",
    "queries":["CREATE view cars AS SELECT * FROM 'https://github.com/keboola/developers-docs/raw/3f1e8a4331638a2300b29e63f797a1d52d64929e/integrate/variables/countries.csv'"],
    "output": ["cars"]
}
```


# Development

This example contains a runnable container with simple unittests. For local testing, it is useful to include the `data` folder
in the root and use docker-compose commands to run the container or execute tests.

If required, change the local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path:

```yaml
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
```

Clone this repository, initialize the workspace, and run the component with the following commands:

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

The preset pipeline scripts contain sections allowing pushing a testing image into the ECR repository and automatic
testing in a dedicated project. These sections are by default commented out.

# Integration

For information about deployment and integration with Keboola, please refer to
the [deployment section of developers' documentation](https://developers.keboola.com/extend/component/deployment/). 
