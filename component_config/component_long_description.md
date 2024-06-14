
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
