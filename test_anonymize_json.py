import unittest
import anonymize_json
import pandas as pd
import os.path
import os
import yaml
import json
import click
import errno
from click.testing import CliRunner
from pathlib import Path


class TestAnonymizeJSON(unittest.TestCase):
    def test_load_json_file(self):
        """Load JSON file
        >>> load_json_file('path/file.json')
        >>> ['foo.json', 'foo.json']
        """
        anonymize_json.load_json_file('raw/foo/foo.json')
        self.assertTrue(os.path.exists('raw/foo/foo.json'))
        self.assertTrue(os.path.isfile('raw/foo/foo.json'))


    def test_json_to_dataframe(self):
        """Read JSON file and return a flat table dataframe
        >>> json_to_dataframe('json_file')
        >>> pandas.DataFrame['key.SubKey.SubKey']
        """
        json_file = anonymize_json.load_json_file('raw/foo/foo.json')
        dataframe = anonymize_json.json_to_dataframe(json_file)
        self.assertIsInstance(dataframe, pd.DataFrame)

    
    def test_set_for_keys(self):
        """Set val at path in my_dict defined by the string (or serializable object) array key_arr
        >>> set_for_keys({}, [], [])
        >>> {}
        """
        my_dict = anonymize_json.set_for_keys({}, [], [])
        self.assertIsInstance(my_dict, dict)


    def test_dataframe_to_json(self):
        """Unflaten pandas dataframe and return JSON dict
        >>> dataframe_to_json('dataframe', sep='.')
        >>> {'key' : 'value'}
        """
        json_file = anonymize_json.load_json_file('raw/foo/foo.json')
        dataframe = anonymize_json.json_to_dataframe(json_file)
        json_dict = anonymize_json.dataframe_to_json(dataframe)
        self.assertIsInstance(json_dict, dict)


    @classmethod
    def setUpClass(cls):
        config_file_name = 'foo_config.yaml'
        
        yaml_data = dict(
            foo = dict(
                regex = '.',
                replace = '*'
            )
        )

        with open(config_file_name, 'w') as file:
            yaml.dump(yaml_data, file, default_flow_style=False)

        if not os.path.exists(os.path.dirname('raw/foo/foo.json')):
            try:
                os.makedirs(os.path.dirname('raw/foo/foo.json'))
            except OSError as e: # Guard against race condition
                if e.errno != errno.EEXIST:
                    raise
        
        with open('raw/foo/foo.json', 'w') as file:
            json.dump({'foo': 'Foo', 'boo': 'Boo'}, file, sort_keys=True, indent=4)


    @classmethod
    def tearDownClass(cls):
        os.remove('foo_config.yaml')
        os.remove('raw/foo/foo.json')
        os.rmdir('raw/foo/')
        os.rmdir('raw/')


if __name__ == '__main__':
    unittest.main()