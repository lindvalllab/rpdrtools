# rpdrtools

This repository contains utility functions for handling [RPDR (Research Patient Data Registry)](https://rc.partners.org/about/who-we-are-risc/research-patient-data-registry) data.

## Installation

TBD

## Example

Reading an RPDR file in to a Pandas DataFrame:

```python
from rpdrtools.io import read_file

path = "PATH/TO/YOUR/RPDR_FILE.txt"
df = read_file(path)
```

## Contributing

This section is for people who want to contribute code to this project.

### Setting up your environment

After cloning this repository:

1. Install [Poetry](https://python-poetry.org/docs/) (dependency management tool for Python). Make sure to configure the path as described when running the install script.
2. In the project folder (the one with `pyproject.toml`), run `poetry install`. This will install all the dependencies you will need, including the current project in [editable mode](https://pip.pypa.io/en/stable/cli/pip_install/#editable-installs).
3. Run `poetry shell` to activate the virtual environment.

That's it! If you to add/remove/update a dependency, please use the `poetry [add/remove/update]` commands.

### Running the tests

To run all the tests, simply run `pytest` in the project directory.
