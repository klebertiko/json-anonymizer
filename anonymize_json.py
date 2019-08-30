"""
Usage:

python3 anonymize_json.py 
--input <input_directory> 
--output <output_directory> 
--config_file <YAML-configuration-file>
--string_data True or False
"""

from jsonpath_rw import jsonpath, parse
from pandas.io.json import json_normalize
import click
import pandas as pd
import json
import yaml
import os
import errno
import re


def load_json_file(json_file_path):
    """
    Loads JSON file
    """
    with open(json_file_path, 'r') as file:
        json_file = file.read()
    
    return json_file


def json_to_dataframe(json_file):
    """
    Read JSON file and return a flat table dataframe
    """
    return json_normalize(json.loads(json_file))


def dataframe_to_json(dataframe, sep="."):
    """
    Unflatten pandas dataframe and return JSON dict
    """
    for _, row in dataframe.iterrows():
        parsed_row = {}
        for idx, val in row.iteritems():
            if val == val:
                keys = idx.split(sep)
                parsed_row = set_for_keys(parsed_row, keys, val)
    return parsed_row


def set_for_keys(my_dict, key_arr, val):
    """
    Set val at path in my_dict defined by the string (or serializable object) array key_arr
    """
    current = my_dict
    for i in range(len(key_arr)):
        key = key_arr[i]
        if key not in current:
            if i==len(key_arr)-1:
                current[key] = val
            else:
                current[key] = {}
        else:
            if type(current[key]) is not dict:
                print("Given dictionary is not compatible with key structure requested")
                raise ValueError("Dictionary key already occupied")

        current = current[key]
    return my_dict


def save_json_file(json_file_path, json_dict):
    """
    Save JSON file
    """
    if not os.path.exists(os.path.dirname(json_file_path)):
        try:
            os.makedirs(os.path.dirname(json_file_path))
        except OSError as e: # Guard against race condition
            if e.errno != errno.EEXIST:
                raise
    
    with open(json_file_path, 'w') as file:
        json.dump(json_dict, file, sort_keys=True, indent=4)


@click.command()
@click.option(
    '--input_json',
    type=click.File('r'),
    required=True,
    help='Input JSON file to be anonymized'
)
@click.option(
    '--output_json',
    type=click.File('w'),
    required=True,
    help='Output anonymized JSON file to be saved'
)
@click.option(
    '--config_file',
    type=click.File('r'),
    required=True,
    help='YAML file to configure anonymization'
)
@click.option(
    '--string_data',
    type=click.BOOL,
    default=False,
    help='Read JSON data as str'
)
def anonymize(input_json, output_json, config_file, string_data):
    """
    Anonymize JSON file
    """
    json_file = load_json_file(input_json.name)
    dataframe = json_to_dataframe(json_file)

    """
    Read data as str
    """
    if string_data:
        dataframe = dataframe.astype(str)
    
    """
    Read YAML file with flat paths
    """
    with open(config_file.name, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
            """
            Parse configurations
            """
            for path in config.keys():
                regex = config[path]['regex']
                replace = config[path]['replace']
                
                # Hack pandas dataframe/series/generic replace for int values
                import ipdb;ipdb.set_trace()
                if 'int64' == dataframe[path].dtypes:
                    dataframe.at[0, path] = re.sub(pattern=regex, repl=str(replace), string=str(dataframe[path][0]))
                    
                """
                Replace with regex
                """                
                dataframe[path].replace(value=replace, regex=regex, inplace=True)
        except yaml.YAMLError as e:
            print(e)

    json_dict = dataframe_to_json(dataframe)
    save_json_file(output_json.name, json_dict)


if __name__ == "__main__":
    anonymize()