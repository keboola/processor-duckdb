import json
import logging
import os
import shutil
from csv import DictReader
from pathlib import Path

import duckdb
from duckdb.duckdb import DuckDBPyConnection
from keboola.component import ComponentBase, UserException
from keboola.component.dao import TableDefinition, TableMetadata, SupportedDataTypes
import fnmatch
from typing import Union

KEY_MODE = "mode"
KEY_IN_TABLES = "input"
KEY_QUERIES = "queries"
KEY_OUT_TABLES = "output"
KEY_DETECT_TYPES = "detect_types"
DUCK_DB_DIR = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'duckdb')

KEY_DEBUG = 'debug'
MANDATORY_PARS = []


class Component(ComponentBase):

    def __init__(self):
        ComponentBase.__init__(self)
        self.validate_configuration_parameters(MANDATORY_PARS)
        self._config = self.configuration.parameters
        self._connection = self.init_connection()
        self._in_tables = self.get_input_tables_definitions()

    def run(self):

        if self._config.get(KEY_MODE) == 'advanced':
            self.advanced_mode()
        else:
            self.simple_mode()

    def init_connection(self) -> DuckDBPyConnection:
        """
                Returns connection to temporary DuckDB database
                """
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        # TODO: On GCP consider changin tmp to /opt/tmp
        config = dict(temp_directory='/opt/tmp/dbtmp',
                      threads="4",
                      memory_limit="512MB",
                      max_memory="512MB")
        conn = duckdb.connect(config=config)

        return conn

    def simple_mode(self):
        """
        Simple mode runs the query on the specified input table.
        Output is stored in the output table with the same name.
        All other tables and all files are moved to the output.
        """
        queries = self._config.get(KEY_QUERIES)

        matched_tables = []
        for q in queries:
            self.run_simple_query(q)

            for t in self._in_tables:
                if fnmatch.fnmatch(t.name, q["input"]):
                    matched_tables.append(t)

        for t in [tb for tb in self._in_tables if tb not in matched_tables] :
            out_table = self.create_out_table_definition(t.name)
            self.move_table_to_out(t, out_table)
            self.move_files()

    def advanced_mode(self):
        """
        Simple mode runs the query on the specified input table.
        Output is stored in the output table with the same name.
        All other tables and all files are moved to the output.
        """
        in_tables = self._config.get(KEY_IN_TABLES)
        queries = self._config.get(KEY_QUERIES)
        out_tables = self._config.get(KEY_OUT_TABLES)

        matched_tables = []
        for t in in_tables:
            self.create_table(t)

            pattern = t
            if isinstance(t, dict):
                pattern = t.get('input_pattern')

            for table in self._in_tables:
                if fnmatch.fnmatch(table.name, pattern):
                    matched_tables.append(t)

        for t in [tb for tb in self._in_tables if tb not in matched_tables]:
            out_table = self.create_out_table_definition(t.name)
            self.move_table_to_out(t, out_table)
            self.move_files()

        for q in queries:
            self._connection.execute(q)

        for o in out_tables:
            if isinstance(o, dict):
                source = o.get('duckdb_source')
                if not source:
                    raise ValueError("Missing source in out_tables definition")
                incremental = o.get('incremental', False)
                primary_key = o.get('primary_key', [])
                destination = o.get('kbc_destination')
                o = source

            else:
                incremental = False
                primary_key = []
                destination = ''

            table_name = o.replace(".csv", "")

            table_meta = self._connection.execute(f"""DESCRIBE TABLE '{o}';""").fetchall()
            cols = [c[0] for c in table_meta]

            tm = TableMetadata()
            tm.add_column_data_types({c[0]: self.convert_base_types(c[1]) for c in table_meta})
            out_table = self.create_out_table_definition(f"{table_name}.csv", columns=cols, table_metadata=tm,
                                                         primary_key=primary_key, incremental=incremental,
                                                         destination=destination)
            self.write_manifest(out_table)

            self._connection.execute(f'''COPY "{table_name}" TO "{out_table.full_path}"
                                        (HEADER, DELIMITER ',', FORCE_QUOTE *)''')

        self._connection.close()
        self.move_files()

    def run_simple_query(self, q: dict):
        self.create_table(q["input"])

        incremental = False
        primary_key = []

        if isinstance(q.get("output"), dict):
            incremental = q["output"].get('incremental', False)
            primary_key = q["output"].get('primary_key', [])

        table_meta = self._connection.execute(f"""DESCRIBE {q["query"]};""").fetchall()
        cols = [c[0] for c in table_meta]

        tm = TableMetadata()
        tm.add_column_data_types({c[0]: self.convert_base_types(c[1]) for c in table_meta})

        if isinstance(q.get("input"), dict):
            destination = q.get("input", {}).get('duckdb_destination') or q.get("input", {}).get("input_pattern")
        else:
            destination = q.get("input")

        if isinstance(q.get("output"), dict):
            destination = q.get("output", {}).get("kbc_destination") or destination
        elif isinstance(q.get("output"), str):
            destination = q.get("output")

        table_name = f"{destination.replace(".csv", "")}.csv"

        out_table = self.create_out_table_definition(table_name, columns=cols,
                                                     table_metadata=tm, primary_key=primary_key,
                                                     incremental=incremental, destination=destination)

        self.write_manifest(out_table)
        self._connection.execute(f'COPY ({q["query"]}) TO "{out_table.full_path}"'
                                 f'(HEADER, DELIMITER ",", FORCE_QUOTE *)')

        logging.debug(f'Table {table_name} export finished.')

    def _get_table_header(self, t: TableDefinition):
        """
        Get table header from the file or from the manifest
        """
        if t.is_sliced or t.columns:
            header = t.columns
        else:
            with open(t.full_path, encoding='utf-8') as input:
                delimiter = t.delimiter
                enclosure = t.enclosure
                reader = DictReader(input, lineterminator='\n', delimiter=delimiter, quotechar=enclosure)
                header = reader.fieldnames

        return header

    def _has_header_in_file(self, t: TableDefinition):
        is_input_mapping_manifest = 'uri' in t._raw_manifest
        has_header = True
        if t.is_sliced:
            has_header = False
        elif t.columns and not is_input_mapping_manifest:
            has_header = False
        else:
            has_header = True
        return has_header

    def create_table(self, input_table_config: Union[dict, str]) -> None:
        itc = input_table_config

        if isinstance(itc, str):
            itc = {'input_pattern': itc}

        if any(char in itc.get("input_pattern") for char in "*?["):
            if not itc.get('duckdb_destination'):
                raise UserException("Destination must be set if input path contains pattern.")
            destination_table_name = [itc["duckdb_destination"]]
            path = itc["input_pattern"]
            table = {}
            has_header = False
            header = False
        else:
            destination_table_name = itc.get('duckdb_destination') or itc["input_pattern"]

            matched_tables = [i for i in self._in_tables if i.name == itc["input_pattern"]]

            if len(matched_tables) == 0:
                raise UserException(f"Table {itc['input_pattern']} not found.")
            elif len(matched_tables) > 1:
                raise UserException(f"Multiple input tables with name {itc['input_pattern']} found.")

            table = matched_tables[0]
            header = itc.get('column_names') or self._get_table_header(table)
            has_header = self._has_header_in_file(table)

            if table.is_sliced:
                path = f'{table.full_path}/*.csv'
            else:
                path = table.full_path

        delimiter = itc.get('delimiter') or table.delimiter or ','
        quote_char = itc.get('quotechar')
        has_header = itc.get('has_header') or has_header
        header = itc.get('column_names') or header
        skip = itc.get('skip_lines')
        date_format = itc.get('date_format')
        timestamp_format = itc.get('timestamp_format')
        add_filename_col = itc.get('add_filename_column')
        detect_dtypes = itc.get('detect_dtypes')

        # dynamically name the relation as a table name so it can be accessed later from the query
        globals()[destination_table_name] = self._connection.read_csv(
            path_or_buffer=path, delimiter=delimiter, quotechar=quote_char, header=has_header, names=header, skiprows=skip,
            date_format=date_format, timestamp_format=timestamp_format, filename=add_filename_col,
            all_varchar=not detect_dtypes)

        logging.debug(f"Table {destination_table_name} created.")

    def move_files(self) -> None:
        files = self.get_input_files_definitions()
        for file in files:
            new_file = self.create_out_file_definition(file.name)
            if os.path.isfile(file.full_path):
                shutil.copy(file.full_path, new_file.full_path)
            elif os.path.isdir(file.full_path):
                shutil.copytree(file.full_path, new_file.full_path, dirs_exist_ok=True)

    def move_table_to_out(self, source, destination):
        if os.path.isfile(source.full_path):
            shutil.copy(source.full_path, destination.full_path)
        elif os.path.isdir(source.full_path):
            shutil.copytree(source.full_path, destination.full_path, dirs_exist_ok=True)
        if Path(f'{source.full_path}.manifest').exists():
            shutil.copy(f'{source.full_path}.manifest', f'{destination.full_path}.manifest')
        else:
            self.write_manifest(destination)

    def convert_base_types(self, dtype: str) -> SupportedDataTypes:
        if dtype in ['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT',
                     'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT', 'UHUGEINT']:
            return SupportedDataTypes.INTEGER
        elif dtype in ['REAL', 'DECIMAL']:
            return SupportedDataTypes.NUMERIC
        elif dtype == 'DOUBLE':
            return SupportedDataTypes.FLOAT
        elif dtype == 'BOOLEAN':
            return SupportedDataTypes.BOOLEAN
        elif dtype in ['TIMESTAMP', 'TIMESTAMP WITH TIME ZONE']:
            return SupportedDataTypes.TIMESTAMP
        elif dtype == 'DATE':
            return SupportedDataTypes.DATE
        else:
            return SupportedDataTypes.STRING

    def _copy_manifest_to_out(self, t: TableDefinition):
        if t.get_manifest_dictionary():
            new_path = os.path.join(self.tables_out_path, Path(t.full_path).name + '.manifest')
            shutil.copy(t.full_path + '.manifest', new_path)

    def add_column_to_manifest_and_copy_to_out(self, file_path, manifest, new_column):
        manifest['columns'].append(new_column)
        with open(os.path.join(self.tables_out_path, Path(file_path).name + '.manifest'), 'w+') as out_f:
            json.dump(manifest, out_f)

    def _copy_table_to_out(self, t: TableDefinition):
        if Path(t.full_path).is_dir():
            shutil.copytree(t.full_path, Path(self.tables_out_path).joinpath(t.name))
        else:
            shutil.copy(t.full_path, Path(self.tables_out_path).joinpath(t.name))


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
