import json
import logging
import os
import shutil
from csv import DictReader
from pathlib import Path

import duckdb
from duckdb.duckdb import DuckDBPyConnection
from keboola.component import ComponentBase, UserException
from keboola.component.dao import TableDefinition, SupportedDataTypes, BaseType, ColumnDefinition, TableMetadata
import fnmatch
from typing import Union
from collections import OrderedDict
from dataclasses import dataclass

KEY_MODE = "mode"
KEY_IN_TABLES = "input"
KEY_QUERIES = "queries"
KEY_OUT_TABLES = "output"
DUCK_DB_DIR = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'duckdb')

KEY_DEBUG = 'debug'
MANDATORY_PARS = []


@dataclass
class InTableParams:
    all_varchar: bool
    destination_table_name: str
    dtypes: dict
    has_header: bool
    header: list
    table_config: dict
    path: str
    table: TableDefinition


@dataclass
class OutTableParams:
    table_name: str
    destination: str
    incremental: bool
    primary_key: list


class Component(ComponentBase):

    def __init__(self):
        ComponentBase.__init__(self)
        self.validate_configuration_parameters(MANDATORY_PARS)
        self._config = self.configuration.parameters
        self._connection = self.init_connection()
        self._in_tables = self.get_input_tables_definitions()

    def set_motherduck_token_from_md_connection(self, md_con):
        """
        Fetches the MotherDuck token using PRAGMA PRINT_MD_TOKEN from the given MotherDuck connection
        and sets it as the 'motherduck_token' environment variable for future use.
        """
        try:
            result = md_con.execute("PRAGMA PRINT_MD_TOKEN;").fetchone()
            if result and result[0]:
                os.environ['motherduck_token'] = result[0]
                logging.info("MotherDuck token set from MotherDuck connection.")
            else:
                logging.warning("No MotherDuck token found in MotherDuck connection.")
        except Exception as e:
            logging.error(f"Failed to fetch MotherDuck token from MotherDuck connection: {e}")
            # Do not raise, just log; user may want to handle token manually

    def ensure_motherduck_token(self, database_name):
        """
        Ensures the motherduck_token is set in the environment.
        If not, connects to MotherDuck, fetches the token using PRAGMA PRINT_MD_TOKEN, sets it, and closes the connection.
        """
        if os.environ.get('motherduck_token'):
            logging.info("MotherDuck token already set in environment.")
            return
        try:
            # Connect to MotherDuck (may prompt for login)
            temp_con = duckdb.connect(f"md:{database_name}")
            result = temp_con.execute("PRAGMA PRINT_MD_TOKEN;").fetchone()
            if result and result[0]:
                os.environ['motherduck_token'] = result[0]
                logging.info("MotherDuck token retrieved and set from MotherDuck connection.")
            else:
                logging.warning("No MotherDuck token found in MotherDuck connection.")
            temp_con.close()
        except Exception as e:
            logging.error(f"Failed to fetch MotherDuck token: {e}")
            # Do not raise, just log; user may want to handle token manually

    def run(self):
        """
        Loads all input tables (CSVs) into MotherDuck using the database name from config.json.
        Ensures the MotherDuck token is set, then connects and uploads tables.
        """
        # Retrieve the token from the environment
        token = os.environ.get('motherduck_token')
        if not token:
            raise UserException("MotherDuck token could not be retrieved from environment.")

        # Get the MotherDuck database name from config
        database_name = self._config.get("database")
        if not database_name:
            raise UserException("Missing 'database' parameter in configuration.")

        # Connect to MotherDuck using the token explicitly in the config
        try:
            md_con = duckdb.connect(f"md:{database_name}", config={"motherduck_token": token})
        except Exception as e:
            logging.error(f"Failed to connect to MotherDuck: {e}")
            raise UserException(f"Failed to connect to MotherDuck: {e}")

        # Print the number of input tables and their details for debugging
        print(f"Number of input tables: {len(self._in_tables)}")
        for t in self._in_tables:
            print(f"Table name: {t.name}, path: {t.full_path}")

        # For each input table, use create_table() to handle advanced logic, then upload to MotherDuck
        for table in self._in_tables:
            table_name = Path(table.name).stem  # Remove .csv extension for MotherDuck
            print(f"Processing and uploading: {table.name} (local table: {table_name})")
            try:
                self.create_table(table.name)
                if os.path.isdir(table.full_path):
                    print(f"Table is sliced: {table.full_path}")
                    csv_path = os.path.join(table.full_path, '*.csv')
                else:
                    csv_path = table.full_path

                # Always drop and reload the table in MotherDuck
                if self.table_exists_in_motherduck(md_con, table_name):
                    print(f"Dropping existing table {table_name} in MotherDuck...")
                    md_con.sql(f"DROP TABLE IF EXISTS {table_name}")
                print(f"Loading table {table_name} into MotherDuck (full load)...")
                md_con.sql(f"CREATE TABLE {table_name} AS SELECT * FROM '{csv_path}'")
                logging.info(f"Table '{table_name}' uploaded to MotherDuck successfully (full load).")
            except Exception as e:
                logging.error(f"Failed to process or upload table '{table_name}': {e}")
                raise UserException(f"Failed to process or upload table '{table_name}' to MotherDuck: {e}")

        # Close the MotherDuck connection
        md_con.close()
        logging.info("All tables uploaded to MotherDuck successfully.")

        # Optionally, you can print a message for the user
        print(f"All input tables have been uploaded to MotherDuck database '{database_name}'.")

    def init_connection(self) -> DuckDBPyConnection:
        """
                Returns connection to temporary DuckDB database
                """
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        # TODO: On GCP consider changin tmp to /opt/tmp
        config = dict(temp_directory=DUCK_DB_DIR,
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
        '''
        for q in queries:
            self.run_simple_query(q)

            if isinstance(q[KEY_IN_TABLES], dict):
                pattern = q[KEY_IN_TABLES].get('input_pattern')
            else:
                pattern = q[KEY_IN_TABLES]

            for t in self._in_tables:
                if fnmatch.fnmatch(t.name, pattern):
                    matched_tables.append(t)
        '''

        for t in [tb for tb in self._in_tables if tb not in matched_tables]:
            out_table = self.create_out_table_definition(t.name)
            self.move_table_to_out(t, out_table)
            self.move_files()

    def advanced_mode(self):
        """
        Simple mode runs the query on the specified input table.
        Output is stored in the output table with the same name.
        All other tables and all files are moved to the output.
        """
        in_tables = self._config.get(KEY_IN_TABLES, [])
        queries = self._config.get(KEY_QUERIES, [])
        out_tables = self._config.get(KEY_OUT_TABLES, [])

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

        for table in out_tables:
            table_params = self.get_out_table_params(table)

            table_meta = self._connection.execute(f"""DESCRIBE TABLE '{table_params.table_name}';""").fetchall()
            schema = OrderedDict((c[0], ColumnDefinition(data_types=BaseType(dtype=self.convert_base_types(c[1]))))
                                 for c in table_meta)

            tm = TableMetadata()
            tm.add_column_data_types({c[0]: self.convert_base_types(c[1]) for c in table_meta})

            out_table = self.create_out_table_definition(f"{table_params.table_name}.csv", schema=schema,
                                                         primary_key=table_params.primary_key,
                                                         incremental=table_params.incremental,
                                                         destination=table_params.destination,
                                                         table_metadata=tm
                                                         )

            try:
                self._connection.execute(f'''COPY "{table_params.table_name}" TO "{out_table.full_path}"
                                            (HEADER, DELIMITER ',', FORCE_QUOTE *)''')
            except duckdb.duckdb.ConversionException as e:
                raise UserException(f"Error during query execution: {e}")

            self.write_manifest(out_table)

        self._connection.close()
        self.move_files()

    def run_simple_query(self, q: dict):
        self.create_table(q[KEY_IN_TABLES])

        incremental = False
        primary_key = []

        if isinstance(q.get(KEY_OUT_TABLES), dict):
            incremental = q[KEY_OUT_TABLES].get('incremental', False)
            primary_key = q[KEY_OUT_TABLES].get('primary_key', [])

        table_meta = self._connection.execute(f"""DESCRIBE {q["query"]};""").fetchall()
        schema = OrderedDict((c[0], ColumnDefinition(data_types=BaseType(dtype=self.convert_base_types(c[1]))))
                             for c in table_meta)

        tm = TableMetadata()
        tm.add_column_data_types({c[0]: self.convert_base_types(c[1]) for c in table_meta})

        input_data = q.get(KEY_IN_TABLES)
        output_data = q.get(KEY_OUT_TABLES)
        if isinstance(input_data, dict):
            destination = input_data.get('duckdb_destination') or input_data.get("input_pattern")
        else:
            destination = input_data

        if isinstance(output_data, dict):
            destination = output_data.get("kbc_destination") or destination
        elif isinstance(output_data, str):
            destination = output_data

        table_name = f"{destination.replace(".csv", "")}.csv"

        out_table = self.create_out_table_definition(table_name,
                                                     schema=schema,
                                                     primary_key=primary_key,
                                                     incremental=incremental,
                                                     destination=destination,
                                                     table_metadata=tm
                                                     )

        try:
            self._connection.execute(f'COPY ({q["query"]}) TO "{out_table.full_path}"'
                                     f'(HEADER, DELIMITER ",", FORCE_QUOTE *)')
        except duckdb.duckdb.ConversionException as e:
            raise UserException(f"Error during query execution: {e}")

        self.write_manifest(out_table)

        logging.debug(f'Table {table_name} export finished.')

    def _get_table_header(self, t: TableDefinition):
        """
        Get table header from the file or from the manifest
        """
        if t.is_sliced or t.column_names:
            header = t.column_names
        else:
            with open(t.full_path, encoding='utf-8') as input:
                delimiter = t.delimiter
                enclosure = t.enclosure
                reader = DictReader(input, lineterminator='\n', delimiter=delimiter, quotechar=enclosure)
                header = reader.fieldnames

        return header

    def _has_header_in_file(self, t: TableDefinition):
        is_input_mapping_manifest = t.stage == 'in'
        if t.is_sliced:
            has_header = False
        elif t.column_names and not is_input_mapping_manifest:
            has_header = False
        else:
            has_header = True
        return has_header

    def create_table(self, input_table_config: Union[dict, str]) -> None:
        table_params = self.get_in_table_params(input_table_config)

        delimiter = table_params.table_config.get('delimiter') or table_params.table.delimiter or ','
        quote_char = table_params.table_config.get('quotechar')
        has_header = table_params.table_config.get('has_header') or table_params.has_header
        header = table_params.table_config.get('column_names') or table_params.header or None
        skip = table_params.table_config.get('skip_lines')
        date_format = table_params.table_config.get('date_format')
        timestamp_format = table_params.table_config.get('timestamp_format')
        add_filename_col = table_params.table_config.get('add_filename_column')

        # dynamically name the relation as a table name so it can be accessed later from the query
        try:
            globals()[table_params.destination_table_name] = self._connection.read_csv(
                path_or_buffer=table_params.path, delimiter=delimiter, quotechar=quote_char, header=has_header,
                names=header, skiprows=skip, date_format=date_format, timestamp_format=timestamp_format,
                filename=add_filename_col, dtype=table_params.dtypes, all_varchar=table_params.all_varchar)

            logging.debug(f"Table {table_params.destination_table_name} created.")
        except duckdb.duckdb.IOException:
            logging.error(f"No files found that match the pattern {table_params.path}")
            exit(0)

    def get_in_table_params(self, table_config) -> InTableParams:
        table = {}
        dtypes = None
        all_varchar = False

        if isinstance(table_config, str):
            table_config = {'input_pattern': table_config}

        if any(char in table_config.get("input_pattern") for char in "*?["):
            if not table_config.get('duckdb_destination'):
                raise UserException("Destination must be set if input path contains pattern.")
            destination_table_name = table_config["duckdb_destination"]
            path = table_config["input_pattern"]
            has_header = False
            header = False
            dtypes_param = table_config.get('dtypes_mode')

            matched_tables = [i for i in self._in_tables if fnmatch.fnmatch(i.name, table_config["input_pattern"])]
            if dtypes_param == 'from_manifest':
                dtypes = {key: value.data_types.get("base").dtype for key, value in matched_tables[0].schema.items()}
            elif dtypes_param == 'all_varchar':
                all_varchar = True

        else:
            destination_table_name = table_config.get('duckdb_destination') or table_config["input_pattern"]
            matched_tables = [i for i in self._in_tables if i.name == table_config["input_pattern"]]

            if len(matched_tables) == 0:
                raise UserException(f"Table {table_config['input_pattern']} not found.")
            elif len(matched_tables) > 1:
                raise UserException(f"Multiple input tables with name {table_config['input_pattern']} found.")

            table = matched_tables[0]
            header = table_config.get('column_names') or self._get_table_header(table)
            has_header = self._has_header_in_file(table)

            dtypes_param = table_config.get('dtypes_mode')

            if dtypes_param == 'from_manifest':
                dtypes = {key: value.data_types.get("base").dtype for key, value in table.schema.items()}
            elif dtypes_param == 'all_varchar':
                all_varchar = True

            if table.is_sliced:
                path = f'{table.full_path}/*.csv'
            else:
                path = table.full_path

        return InTableParams(all_varchar, destination_table_name, dtypes, has_header, header, table_config, path, table)

    def get_out_table_params(self, out_table_config: Union[dict, str]) -> OutTableParams:
        if isinstance(out_table_config, dict):
            source = out_table_config.get('duckdb_source')
            if not source:
                raise ValueError("Missing source in out_tables definition")
            incremental = out_table_config.get('incremental', False)
            primary_key = out_table_config.get('primary_key', [])
            destination = out_table_config.get('kbc_destination')
            table_name = source.replace(".csv", "")

        else:
            incremental = False
            primary_key = []
            destination = ''
            table_name = out_table_config.replace(".csv", "")

        return OutTableParams(table_name, destination, incremental, primary_key)

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

    def table_exists_in_motherduck(self, md_con, table_name):
        """
        Check if a table exists in the MotherDuck database.
        Uses the information_schema.tables system view to check for table existence.
        
        Args:
            md_con: MotherDuck connection object
            table_name: Name of the table to check
            
        Returns:
            bool: True if table exists, False otherwise
        """
        try:
            # Query the information schema to check if the table exists
            # We check both the table_name and table_schema (defaulting to 'main')
            schema_name = self._config.get("schema", "main")
            
            result = md_con.execute("""
                SELECT COUNT(*) as table_count 
                FROM information_schema.tables 
                WHERE table_name = ? AND table_schema = ?
            """, [table_name, schema_name]).fetchone()
            
            # If count > 0, table exists
            table_exists = result[0] > 0 if result else False
            
            logging.debug(f"Table '{table_name}' existence check in schema '{schema_name}': {table_exists}")
            return table_exists
            
        except Exception as e:
            # If there's an error checking table existence, log it and assume table doesn't exist
            # This is safer than assuming it exists and potentially causing data loss
            logging.warning(f"Error checking if table '{table_name}' exists in MotherDuck: {e}")
            return False


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
