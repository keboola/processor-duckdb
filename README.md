# Add Columns processor

Takes all tables in `in/tables/` and adds columns with specified value. Result is moved to `out/tables/`

Files folder `in/files` is moved unchanged.

Manifest files are respected and transferred / modified as well.

**Table of contents:**

[TOC]

## Configuration

list columns that will be added to all tables on the input. Column value is constant provided by specified function.

- **name** - name of the result column
- **function** - function (or set of nested functions) to construct the result value


### Sample configuration

```json
{
  "definition": {
    "component": "kds-team.processor-add-columns"
  },
  "parameters": {
    "columns": [
      {
        "name": "timestamp_custom",
        "function": {
          "function": "string_to_date",
          "args": [
            "yesterday",
            "%Y-%m-%d"
          ]
        }
      }
    ]
  }
}

```

**Functions**

Column values can be filled using functions. These functions can be combined, nested.

```json
{
  "function": "string_to_date",
  "args": [
    "yesterday",
    "%Y-%m-%d"
  ]
}
```

#### Function Nesting

Nesting of functions is supported:

```json
{
  "definition": {
    "component": "kds-team.processor-add-columns"
  },
  "parameters": {
    "columns": [
      {
        "name": "timestamp_custom",
        "function": {
          "function": "concat",
          "args": [
            "custom_timestamp_",
            {
              "function": "string_to_date",
              "args": [
                "yesterday",
                "%Y-%m-%d"
              ]
            }
          ]
        }
      }
    ]
  }
}
```

It todays UTC time is `2022-01-04`, the above will result in column value `custom_timestamp_2022-01-03`

#### string_to_date

Function converting string value into a datestring in specified format. The value may be either date in `YYYY-MM-DD`
format, or a relative period e.g. `5 hours ago`, `yesterday`,`3 days ago`, `4 months ago`, `2 years ago`, `today`.

**The resulting relative time is in UTC timezone**

The result is returned as a date string in the specified format, by default `%Y-%m-%d`

The function takes two arguments:

1. [REQ] Date string
2. [OPT] result date format. The format should be defined as in http://strftime.org/

**Example**

```json
{
  "definition": {
    "component": "kds-team.processor-add-columns"
  },
  "parameters": {
    "columns": [
      {
        "name": "timestamp_custom",
        "function": {
          "function": "string_to_date",
          "args": [
            "yesterday",
            "%Y-%m-%d"
          ]
        }
      }
    ]
  }
}
```

#### concat

Concatenate an array of strings.

The function takes an array of strings to concatenate as an argument

**Example**

```json
{
  "definition": {
    "component": "kds-team.processor-add-columns"
  },
  "parameters": {
    "columns": [
      {
        "name": "url_concat",
        "function": {
          "function": "concat",
          "args": [
            "http://example.com",
            "/test"
          ]
        }
      }
    ]
  }
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