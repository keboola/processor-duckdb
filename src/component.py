import json
import logging
import os
import shutil
from csv import DictReader
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
from duckdb.duckdb import DuckDBPyConnection
from keboola.component import ComponentBase, UserException
from keboola.component.dao import TableDefinition, TableMetadata, SupportedDataTypes

KEY_MODE = "mode"
KEY_IN_TABLES = "in_tables"
KEY_QUERIES = "queries"
KEY_OUT_TABLES = "out_tables"
DUCK_DB_DIR = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'duckdb')

KEY_DEBUG = 'debug'
MANDATORY_PARS = []


@dataclass
class TableDef:
    path: str
    file_name: str
    is_sliced: bool
    manifest: dict = field(default_factory=dict)


class Component(ComponentBase):

    def __init__(self):
        ComponentBase.__init__(self)
        self.validate_configuration_parameters(MANDATORY_PARS)
        self._config = self.configuration.parameters
        self._connection = self.init_connection()
        # initialize instance parameters

    def run(self):

        if self._config[KEY_MODE] == 'advanced':
            self.advanced_mode()
        else:
            self.simple_mode()

    def init_connection(self) -> DuckDBPyConnection:
        """
                Returns connection to temporary DuckDB database
                """
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        # TODO: On GCP consider changin tmp to /opt/tmp
        config = dict(temp_directory='/tmp/dbtmp',
                      threads="4",
                      memory_limit="2GB'",
                      max_memory="'2GB'")
        conn = duckdb.connect(config=config)

        return conn

    def simple_mode(self):
        """
        Simple mode runs the query on the specified input table.
        Output is stored in the output table with the same name.
        All other tables and all files are moved to the output.
        """
        tables = self.get_input_tables_definitions()
        queries = self._config[KEY_QUERIES]

        for t in tables:
            if t.name in queries.keys():
                self.run_simple_query(t, queries[t.name])

            else:
                out_table = self.create_out_table_definition(t.name)
                self.move_table_to_out(t, out_table)
        self.move_files()

    def advanced_mode(self):
        """
        Simple mode runs the query on the specified input table.
        Output is stored in the output table with the same name.
        All other tables and all files are moved to the output.
        """
        tables = self.get_input_tables_definitions()
        in_tables = self._config[KEY_IN_TABLES]
        queries = self._config[KEY_QUERIES]
        out_tables = self._config[KEY_OUT_TABLES]

        if not in_tables:
            for t in tables:
                # dynamically name the relation as a table name so it can be accessed later from the query
                table_name = t.name.replace(".csv", "")
                vars()[table_name] = self.create_table(t, self._config.get('detect_types', False))

        else:
            for t in tables:
                if t.name in in_tables:
                    # dynamically name the relation as a table name so it can be accessed later from the query
                    table_name = t.name.replace(".csv", "")
                    vars()[table_name] = self.create_table(t, self._config.get('detect_types', False))

        for q in queries:
            self._connection.execute(q)

        for t in tables:
            if t.name not in out_tables:
                out_table = self.create_out_table_definition(t.name)
                self.move_table_to_out(t, out_table)

        for o in out_tables:
            table_meta = self._connection.execute(f"""DESCRIBE TABLE '{o}';""").fetchall()
            cols = [c[0] for c in table_meta]

            tm = TableMetadata()
            tm.add_column_data_types({c[0]: self.convert_base_types(c[1]) for c in table_meta})
            out_table = self.create_out_table_definition(f"{o}.csv", columns=cols, table_metadata=tm)
            self.write_manifest(out_table)

            self._connection.execute(f"COPY '{o}' TO '{out_table.full_path}' (HEADER, DELIMITER ',')")

        self._connection.close()
        self.move_files()

    def run_simple_query(self, input_table: TableDefinition, query: str):
        detect_types = self._config.get('detect_types', False)
        # dynamically name the relation as a table name so it can be accessed later from the query
        table_name = input_table.name.replace(".csv", "")
        vars()[table_name] = self.create_table(input_table, detect_types)

        logging.info(f"Table {table_name} created.")

        table_meta = vars()[table_name].description
        cols = [c[0] for c in table_meta]
        tm = TableMetadata()

        # TODO: here we want to compare source manifest and just amend the types.
        # if detect_types=True, we should overwrite the types
        # type detection should be configurable, by default on
        tm.add_column_data_types({c[0]: self.convert_base_types(c[1]) for c in table_meta})
        out_table = self.create_out_table_definition(f"{table_name}.csv", table_metadata=tm)

        self.write_manifest(out_table)
        self._connection.execute(f"COPY ({query}) TO '{out_table.full_path}' (HEADER , DELIMITER ',')")
        logging.info(f"Table {table_name} export finished.")

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
            has_header = has_header
        elif t.columns and not is_input_mapping_manifest:
            has_header = False
        else:
            has_header = True
        return has_header

    def create_table(self, table: TableDefinition,
                     detect_datatypes: bool = True) -> duckdb.DuckDBPyRelation:

        header = self._get_table_header(table)
        has_header = self._has_header_in_file(table)
        if table.is_sliced:
            path = f'{table.full_path}/*.csv'
        else:
            path = table.full_path

        rel = self._connection.read_csv(path, delim=table.delimiter, quote=table.enclosure, header=has_header,
                                        names=header,
                                        all_varchar=not detect_datatypes)

        return rel

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
        # TODO: You're missing DECIMAL=> numeric types, this should map
        # unfortunately we need to map all types https://duckdb.org/docs/sql/data_types/overview
        if dtype == 'INTEGER':
            return SupportedDataTypes.INTEGER
        elif dtype == 'FLOAT':
            return SupportedDataTypes.FLOAT
        elif dtype == 'BOOLEAN':
            return SupportedDataTypes.BOOLEAN
        elif dtype == 'TIMESTAMP':
            return SupportedDataTypes.TIMESTAMP
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

    def cleanup_duckdb(self):
        # cleanup duckdb (useful for local dev,to clean resources)
        if os.path.exists(DUCK_DB_DIR):
            shutil.rmtree(DUCK_DB_DIR, ignore_errors=True)


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
