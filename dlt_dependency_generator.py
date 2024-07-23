import os
import re

## PARAMETERS TO CHANGE

sandbox_schema = "sergio"

# MODELS THAT YOU WILL SEARCH THE DEPENDENCIES FOR
sources_to_search = [
    'zabit_partners'
]
base_path = os.path.expanduser("~/projects/data-analytics-dbt") # LOCATION OF THE ANALYTICS REPO IN YOUR COMPUTER
pii_access = 0 # CHANGE TO 1 IF YOU HAVE ACCESS TO PII TABLES

## END PARAMETERS TO CHANGE


redacted_tables = [
    "mx_client_services.card_delivering",
    "mx_client_services.cards",
    "mx_client_services.users",
    "mx_operations.authorization_operations",
    "mx_operations.card_account_contract_data",
    "mx_operations.cards",
    "mx_operations.digital_card_device_informations",
    "mx_tpps_schema.beneficiaries",
    "mx_tpps_schema.deposit_transfers",
    "mx_tpps_schema.transfer_invoices",
    "mx_tpps_schema.transfers"
]

dep_directories = [f"{base_path}/{_dir}" for _dir in ["models/staging", "models/intermediate", "models/marts"]]

all_files = []
for _dir in dep_directories:
    for root, _, files in os.walk(_dir):
        for file in files:
            if file.endswith(".sql"):
                all_files.append(os.path.join(root, file))

debug = 0
def debug_echo(message):
    if debug:
        print(message)

def dep_search(mart_dep_table_name, dep_files):
    debug_echo(f"Searching dependencies for: {mart_dep_table_name}")
    found = 0
    for model_file in all_files:
        with open(model_file, 'r') as file:
            content = remove_sql_comments(file.read())
            if re.search(fr"CREATE( OR REFRESH)?( TEMPORARY)? LIVE (VIEW|TABLE) {mart_dep_table_name}\b", content):
                found = 1
                debug_echo(f"found dependency {mart_dep_table_name} in {model_file}")
                dep_files.append(model_file)
                model_dep_table_names = re.findall(r'LIVE\.[a-zA-Z0-9_]*', content)
                model_dep_table_names = [name.split('.')[1] for name in model_dep_table_names]

                for model_dep_table_name in model_dep_table_names:
                    dep_search(model_dep_table_name, dep_files)
    if not found:
        print("-- DEPENDENCIES NOT FOUND FOR", mart_dep_table_name)


def get_dep_files(sources_to_search):
    dep_files = []
    source_files = []
    for dep_directory in dep_directories:
        for root, _, files in os.walk(dep_directory):
            for file in files:
                for source_to_search in sources_to_search:
                    with open(os.path.join(root, file), 'r') as content:
                        content = content.read()
                        if re.search(fr"CREATE( OR REFRESH)?( TEMPORARY)? LIVE (VIEW|TABLE) {source_to_search}\b", content) or file == f"{source_to_search}.sql":
                            source_files.append(os.path.join(root, file))

    for source_file in source_files:
        debug_echo(f"Dependency files for {source_file}:")

        # Extract table names from the model file
        with open(source_file, 'r') as file:
            content = remove_sql_comments(file.read())
            dep_table_names = re.findall(r'LIVE\.[a-zA-Z0-9_]*', content)
            dep_table_names = [name.split('.')[1] for name in dep_table_names]

        # Loop through each table name and search for dependencies in other model files
        for dep_table_name in dep_table_names:
            dep_search(dep_table_name, dep_files)
    
    source_files = [file for file in source_files if file not in dep_files]
    result = source_files + dep_files
    deduplicated = []
    for file in result[::-1]:
        if file not in deduplicated:
            deduplicated.append(file)
    return deduplicated


def remove_sql_comments(text):
    return re.sub(r"--.*", "", text)

def get_sql(sources_to_search):
    sql = ""
    dep_files = get_dep_files(sources_to_search)
    for dep in dep_files:
        with open(dep, 'r') as file:
            content = file.read()
            content = re.sub(f"-- Databricks notebook source","", content)
            content = re.sub(f"CREATE( OR REFRESH)?( TEMPORARY)? LIVE (VIEW|TABLE) ",
                             f"CREATE OR REPLACE TABLE sandbox.{sandbox_schema}.",
                             content)
            content = re.sub("LIVE.",
                             f"sandbox.{sandbox_schema}.",
                             content)
            if not pii_access:
                for redacted in redacted_tables:
                    content = re.sub(fr"production.{redacted}\b",
                                 f"production.{redacted}_redacted",
                                 content)
            sql += content + ";"
    return sql

def get_pretty_dependencies(sources_to_search, remove_sql_extension = True, sort_deps = True, print_deps = True, print_spaces = 4):
    dep_files = get_dep_files(sources_to_search)
    if len(sources_to_search) > 1:
        print("-- Warning: you're getting the dependencies of more than 1 source")
    for i, item in enumerate(dep_files):
        if remove_sql_extension and '.sql' in dep_files[i]:
            dep_files[i] = dep_files[i][:-4]
    dep_files = list(dict.fromkeys(dep_files))
    if sort_deps:
        dep_files.sort()
        dep_files = [a for a in dep_files if '/staging/' in a] + [a for a in dep_files if '/intermediate/' in a] + [a for a in dep_files if '/marts/' in a]
    
    if print_deps:
        print_spaces = " " * print_spaces
        for i, dep in enumerate(dep_files):
            dep = dep.split(f"{base_path}/")[1]
            comma = ',' if i < len(dep_files) - 1 else ''
            print(f"{print_spaces}\"{dep}\"{comma}")
    
    return dep_files

print(get_sql(sources_to_search))
# get_pretty_dependencies(sources_to_search)
