# rpdrtools

This repository contains utility functions and recipes for handling [RPDR (Research Patient Data Registry)](https://rc.partners.org/about/who-we-are-risc/research-patient-data-registry) data.

## Installation

```
pip install git+https://github.com/lindvalllab/rpdrtools.git
```

## Example

Reading an RPDR file in to a Pandas DataFrame:

```python
from rpdrtools.io import read_file

path = "PATH/TO/YOUR/RPDR_FILE.txt"
df = read_file(path)
```

Output:

```
      EMPI EPIC_PMRN MRN_Type     MRN ...
0   012345    012345      MGH  543210 ...
1   456789    456789      MGH  987654 ...
```

## Contributing

Besides [contributing to the repo itself](CONTRIBUTING.md), there are many ways to contribute to this project:

- :star: Star this repo
- [Report a bug](https://github.com/lindvalllab/rpdrtools/issues/new?assignees=&labels=bug&template=bug_report.md&title=)
- [Suggest an enhancement](https://github.com/lindvalllab/rpdrtools/issues/new?assignees=&labels=enhancement&template=feature_request.md&title=)
