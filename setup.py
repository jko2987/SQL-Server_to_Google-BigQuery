import sys
import os
import pkg_resources
import setuptools
from cx_Freeze import setup, Executable

site_pkg = os.listdir('site-packages')
site_pkg_dir = os.getcwd() + '/site-packages/'


# Dependencies are automatically detected, but it might need fine tuning.
build_exe_options = {"packages": ['os', 'sys', 're', 'ast', 'tqdm', 'pyodbc', 'pandas', 'pandas_gbq', 'google.oauth2', 'pkg_resources', 'httplib2', 'numpy','six', 'pytz', 'dateutil', 'requests'],\
                     'include_files': ['bq_creds.json', 'gcp-sa.json', 'db_map.json', 'README.txt', 'log/', 'SQL_Settings_Example/', 'template_files/'] + [site_pkg_dir + x for x in site_pkg]}


# GUI applications require a different base on Windows (the default is for a
# console application).
base = None

# if sys.platform == "win32":
#     base = "Win32GUI"

setup(
    name = "sql_to_bq",
    version = "0.4",
    description = "SQL to BigQuery Automation",
    options = {"build_exe": build_exe_options},
    executables = [Executable("sql_to_bq.py", base=base)],
)