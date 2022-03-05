# Contributing

This section is for people who want to contribute source code to this project.

## Before you get started

Leave a comment in the [issue](https://github.com/lindvalllab/rpdrtools/issues) saying you would like to work on it. If there is no issue for it yet, create one using the appropriate template.

This helps us make sure that multiple people aren't working on the same thing, and that the issue is something we actually want to implement or fix at the moment.

## Setting up your environment

After cloning this repository:

1. Install [Poetry](https://python-poetry.org/docs/) (dependency management tool for Python). Make sure to configure the path as described when running the install script.
2. In the project folder (the one with `pyproject.toml`), run `poetry install`. This will install all the dependencies you will need, including the current project in [editable mode](https://pip.pypa.io/en/stable/cli/pip_install/#editable-installs). (This may take a few minutes.)
3. Run `poetry shell` to activate the virtual environment.

That's it!

## Running the tests

To run all the tests, simply run `pytest` in the project directory.
