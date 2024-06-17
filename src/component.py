import json
import logging
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from keboola.component import ComponentBase, UserException
from keboola.component.dao import TableDefinition, TableMetadata, SupportedDataTypes
import duckdb
from duckdb.duckdb import DuckDBPyConnection

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
        # initialize instance parameters

    def run(self):
        self.cleanup_duckdb()

        if self._config[KEY_MODE] == 'advanced':
            self.advanced_mode()
        else:
            self.simple_mode()

        self.cleanup_duckdb()

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

        conn = self.db_connection()

        if not in_tables:
            for t in tables:
                self.create_table(conn, t)

        else:
            for t in tables:
                if t.name in in_tables:
                    self.create_table(conn, t)

        for q in queries:
            conn.execute(q)

        for t in tables:
            if t.name not in out_tables:
                out_table = self.create_out_table_definition(t.name)
                self.move_table_to_out(t, out_table)

        for o in out_tables:
            table_meta = conn.execute(f"""DESCRIBE TABLE '{o}';""").fetchall()
            cols = [c[0] for c in table_meta]

            tm = TableMetadata()
            tm.add_column_data_types({c[0]: self.convert_base_types(c[1]) for c in table_meta})
            out_table = self.create_out_table_definition(f"{o}.csv", columns=cols, table_metadata=tm)
            self.write_manifest(out_table)

            conn.execute(f"COPY '{o}' TO '{out_table.full_path}' (HEADER, DELIMITER ',')")

        conn.close()
        self.move_files()

    def run_simple_query(self, input_table: TableDefinition, query: str):
        table_name = input_table.name.replace(".csv", "")

        with self.db_connection() as conn:
            self.create_table(conn, input_table)

            conn.execute(f"CREATE OR REPLACE TABLE '{table_name}' AS {query};")
            table_meta = conn.execute(f"""DESCRIBE TABLE '{table_name}';""").fetchall()
            cols = [c[0] for c in table_meta]

            tm = TableMetadata()
            tm.add_column_data_types({c[0]: self.convert_base_types(c[1]) for c in table_meta})
            out_table = self.create_out_table_definition(f"{table_name}.csv", columns=cols, table_metadata=tm)
            self.write_manifest(out_table)

            conn.execute(f"COPY '{table_name}' TO '{out_table.full_path}' (HEADER, DELIMITER ',')")

    def create_table(self, conn: DuckDBPyConnection, table: TableDefinition) -> None:

        table_name = table.name.replace(".csv", "")
        if table.is_sliced:
            path = f'{table.full_path}/*.csv'
        else:
            path = table.full_path

        # read_csv = f"""read_csv('{path}', delim='{table.delimiter}', quote='{table.enclosure}')"""
        read_csv = f"""read_csv_auto('{path}')"""
        query = f"CREATE OR REPLACE TABLE '{table_name}' AS SELECT * FROM {read_csv};"
        conn.execute(query)
        table_meta = conn.execute(f"""DESCRIBE TABLE '{table_name}';""").fetchall()

        for old, new in zip(table_meta, table.columns):
            if old[0] != new:
                conn.execute(f"ALTER TABLE '{table_name}' RENAME COLUMN '{old[0]}' TO '{new}';")

    def db_connection(self) -> DuckDBPyConnection:
        """
        Returns connection to temporary DuckDB database
        """
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        conn = duckdb.connect(database=os.path.join(DUCK_DB_DIR, 'db.duckdb'), read_only=False)
        conn.execute("SET temp_directory	='/tmp/dbtmp'")
        conn.execute("SET threads TO 1")
        conn.execute("SET memory_limit='2GB'")
        conn.execute("SET max_memory='2GB'")
        return conn

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
