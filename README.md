# DuckDB processor

DuckDB processor is a component that allows running SQL queries on DuckDB database. The component is based on the

## Configuration

list columns that will be added to all tables on the input. Column value is constant provided by specified function.

- **mode** - if simple, query for specified tables will be processed and output as table with same name, if advanced, queries will be processed and output will be saved as specified in out_tables 
- **function** - function (or set of nested functions) to construct the result value
- **detect_types** - if set to true, base data types for all columns in manifest of output tables will be based on csv auto detection, if false only new columns which dtypes are not specified in manifest of input tables

### Example configuration

simple mode, for selected table run query and output the result as table with same name
`{
    "mode": "simple",
    "queries":
      {
        "products.csv" : "SELECT *, 'csv' as new_column FROM products AS p LEFT JOIN '/data/in/files/categories.parquet' AS cat on p.category = cat.id ORDER BY p.id"
      }
}`

advanced mode, creates [relation](https://duckdb.org/docs/api/python/relational_api) from specified input tables, run all queries in the list and saves to the output all tables specified on the out tables list
`{
                "mode": "advanced",
                "in_tables": ["products"],
                "detect_types": "true",
                "queries":["CREATE view out AS SELECT * FROM products AS p LEFT JOIN '/data/in/files/categories.parquet' AS cat on p.category = cat.id ORDER BY p.id;"],
                "out_tables": ["out"]
}`

load file from storage and export it as table
`{
    "mode": "advanced",
    "queries":["CREATE view out AS SELECT * FROM '/data/in/files/test.parquet'"],
    "out_tables": ["out"]
}`

load file from url and save it as table
`{
    "mode": "advanced",
    "queries":["CREATE view cars AS SELECT * FROM 'https://github.com/keboola/developers-docs/raw/3f1e8a4331638a2300b29e63f797a1d52d64929e/integrate/variables/countries.csv'"],
    "out_tables": ["cars"]
}`


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