import yaml
import pyodbc
import struct
from itertools import chain, repeat
from collections import OrderedDict
from azure.identity import AzureCliCredential
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_yaml_file(filename):
    try:
        with open(filename, 'r') as file:
            data = yaml.safe_load(file)
        return data
    except Exception as e:
        logging.error(f"Error reading YAML file: {e}")
        raise

def ordered_yaml_dump(data, stream=None, **kwds):
    class OrderedDumper(yaml.SafeDumper):
        pass
    def _dict_representer(dumper, data):
        return dumper.represent_mapping(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, data.items())
    OrderedDumper.add_representer(OrderedDict, _dict_representer)
    return yaml.dump(data, stream, OrderedDumper, **kwds)

def write_yaml_file(filename, data):
    try:
        with open(filename, 'w') as file:
            ordered_yaml_dump(data, file)
            file.flush()
    except Exception as e:
        logging.error(f"Error writing YAML file: {e}")
        raise

def execute_query(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        raise

data = read_yaml_file('d365_tables.yml')

for source in data['sources']:
    logging.info(f"Processing source: {source['name']}")
    logging.info(f"Source Lakehouse: {source['source_lakehouse']}")
    logging.info(f"Target yml file: {source['target_yml']}")
    logging.info(f"Tables: {source['tables']}")

    # Establish a connection to the Microsoft Fabric Lakehouse
    credential = AzureCliCredential()
    sql_endpoint = "b27nglr6dgderlhlbqidfj2kge-pwo43isi2ezetbspsg37pqmaqa.datawarehouse.fabric.microsoft.com"
    database = source['source_lakehouse']
    connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_endpoint},1433;Database={database};Encrypt=Yes;TrustServerCertificate=No"

    # prepare the access token
    token_object = credential.get_token("https://database.windows.net//.default")
    token_as_bytes = bytes(token_object.token, "UTF-8")
    encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))
    token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes
    attrs_before = {1256: token_bytes}

    # build the connection
    try:
        connection = pyodbc.connect(connection_string, attrs_before=attrs_before)
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise

    # Initialize the template data structure
    template_data = OrderedDict([
        ("version", 2),
        ("sources", [
            OrderedDict([
                ("name", source['name']),
                ("schema", source['schema']),
                ("database", source['source_lakehouse']),
                ("description", source['description']),
                ("tables", [])
            ])
        ])
    ])

    for table in source['tables']:
        query = f"SELECT COLUMN_NAME, DATA_TYPE FROM {source['source_lakehouse']}.[INFORMATION_SCHEMA].[COLUMNS] WHERE TABLE_NAME = '{table}'"
        result = execute_query(connection, query)
        logging.info(f"Processing: [{table}] ")
        logging.info(result)

        # Initialize the table data structure
        table_data = OrderedDict([("name", table), ("columns", [])])

        for row in result:
            # Append the column data to the table data structure
            table_data["columns"].append(OrderedDict([("name", row[0]), ("data_type", row[1])]))
        
        # Append the table data to the template data
        template_data["sources"][0]["tables"].append(table_data)

    # Write the template data to the sources.yml file
    write_yaml_file(source['target_yml'], template_data)

    # Print a success message
    logging.info(f"The template data was successfully written to {source['target_yml']} file.")
