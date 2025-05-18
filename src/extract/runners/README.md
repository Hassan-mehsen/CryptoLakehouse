# runners/

This folder contains local test scripts for data extractors.

Each file here allows individual testing of an extractor (by endpoint)
without going through Airflow. This makes it easier to develop, debug, and validate
API calls locally.

## Structure

- `__init__.py` : makes this folder importable as a package
- `example_test.py` *(not versioned)*: typical local test file
- `README.md` : this file

The .py files are ignored by the Git repository via .gitignore to avoid cluttering
the repository with temporary or non-production-executable code.

You are free to add your own tests here to develop more efficiently.
