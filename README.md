# DuckDB processor

DuckDB processor is a component that allows running SQL queries on DuckDB database. For more information about DuckDB, please visit the [DuckDB Docs website](https://duckdb.org/docs/).

## Configuration

Component supports two modes of operation, simple and advanced.
- **mode** - "simple" (default) / "advanced"


### Simple mode
In simple mode, each query operates on a single input table (along with an arbitrary number of files), and the query output is exported using the name of the input table.

In basic mode, you have the option to utilize the following parameters:
- **detect_types** - false (default) / true
  - When set to true, data types for all columns will be inferred. If set to false, only new columns or columns without specified data types in the input manifest will be inferred.
- **queries** - A dictionary of queries to be executed. The key is the name of the input table both name or name.csv are supported. The value is the SQL query to be executed.

#### Example configuration

```
{
    "queries":
      {
        "sales" : "SELECT sales_representative, SUM(turnover) AS total_turnover FROM sales GROUP BY sales_representative;",
      }
}
```

```
{
    "mode": "simple",
    "queries":
      {
        "products.csv" : "SELECT *, 'csv' as new_column FROM products AS p LEFT JOIN '/data/in/files/categories.parquet' AS cat on p.category = cat.id ORDER BY p.id",
        "categories" : "SELECT * FROM categories"
      }
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
- **out_tables** - A list of output tables to be exported. Each table can be specified as a string or a dictionary. If a dictionary is used, the key is the table name and the value is a dictionary with optional parameters:
  - **primary_keys** - A list of primary keys for the table.
  - **incremental** - (Default: false) When set to true, the table is exported incrementally.


#### Example configuration


```
{
    "mode": "advanced",
    "in_tables": ["products"],
    "detect_types": "true",
    "queries":["CREATE view out AS SELECT * FROM products AS p LEFT JOIN '/data/in/files/categories.parquet' AS cat on p.category = cat.id ORDER BY p.id;",
               "CREATE view out2 AS SELECT * FROM products WHERE discount = TRUE;"],
    "out_tables": ["out", {"second": { "primary_keys": ["id", "day"], "incremental": true}}],
}
```

Load file from url and save it as table
```
{
    "mode": "advanced",
    "queries":["CREATE view cars AS SELECT * FROM 'https://github.com/keboola/developers-docs/raw/3f1e8a4331638a2300b29e63f797a1d52d64929e/integrate/variables/countries.csv'"],
    "out_tables": ["cars"]
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