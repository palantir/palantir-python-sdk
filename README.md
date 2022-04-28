# Palantir Python SDK
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/palantir-sdk)
[![PyPI](https://img.shields.io/pypi/v/palantir-sdk)](https://pypi.org/project/palantir-sdk/)
[![License](https://img.shields.io/badge/License-Apache%202.0-lightgrey.svg)](https://opensource.org/licenses/Apache-2.0)
[![Autorelease](https://img.shields.io/badge/Perform%20an-Autorelease-success.svg)](https://autorelease.general.dmz.palantir.tech/palantir/palantir-python-sdk)

This SDK is incubating and subject to change.

## Setup

```commandline
pip install palantir-sdk
```

```commandline
conda config --add channels conda-forge  # add conda-forge channel if not already enabled
conda install palantir-sdk
mamba install palantir-sdk  # alternatively install with mamba
```

Configuration for hostname and an authentication token are provided by environment variables (`PALANTIR_HOSTNAME`, `PALANTIR_TOKEN`)

* `PALANTIR_HOSTNAME` is the hostname of your instance e.g. `example.palantirfoundry.com`
* `PALANTIR_TOKEN` is a token acquired from the `Tokens` section of Foundry Settings 
 
Authentication tokens serve as a private password and allows a connection to Foundry data. Keep your token secret and do not share it with anyone else. Do not add a token to a source controlled or shared file.

## Examples

### Read a Foundry Dataset into a Pandas DataFrame
```python
from palantir.datasets import dataset

dataset("/Path/to/dataset") \
    .read_pandas()
```

```
            id        word  length     double boolean
0            0           A     1.0  11.878200       1
1            1           a     1.0  11.578800       0
2            2          aa     2.0  15.738500       1
3            3         aal     3.0   6.643900       0
4            4       aalii     5.0   2.017730       1
...        ...         ...     ...        ...     ...
235881  235881      zythem     6.0  19.427400       1
235882  235882      Zythia     6.0  14.397100       1
235883  235883      zythum     6.0   3.385820       0
235884  235884     Zyzomys     7.0   6.208830       1
235885  235885  Zyzzogeton    10.0   0.947821       0

[235886 rows x 5 columns]
```

### Write a Pandas DataFrame to a Foundry Dataset
```python
import pandas as pd
from palantir.datasets import dataset

df = pd.DataFrame({
    "string": ["one", "two"],
    "integer": [1, 2]
})

ds = dataset(f"/Path/to/dataset", create=True)
ds.write_pandas(df)
```

### List files in a Dataset
```python
from palantir.datasets import dataset

files = dataset("/Path/to/dataset") \
    .list_files() # returns a generator over pages of files

list(files)
```

```
[
    File("ri.foundry.main.dataset.2ed83c69-e87e-425e-9a1c-03b77b5b0831", "file.txt")
]
```

### Read the contents of a file from a dataset (by name)
```python
from palantir.datasets import dataset

dataset("/Path/to/dataset") \
    .file("file.txt") \
    .read()
```
```python
b'Hello!'
```

### Read the contents of a file from a dataset (by exploration / listing)
```python
from palantir.datasets import dataset

files = dataset("/Path/to/dataset").list_files()
next(files).read()
```
```
b'Hello!'
```

### Dataset functions also accept Resource Identifiers (rids)
```python
from palantir.datasets import dataset

dataset("ri.foundry.main.dataset.a0a94f00-754e-49ff-a4f6-4f5cc200d45d") \
    .read_pandas()
```
```
  string  integer
0    one        1
1    two        2
```

## Contributing

See the [CONTRIBUTING.md](./CONTRIBUTING.md) document.  Releases are published to [pypi](https://pypi.org/project/palantir-sdk/) on tag builds and are automatically re-published to [conda](https://anaconda.org/conda-forge/palantir-sdk) using [conda-forge](https://github.com/conda-forge/palantir-sdk-feedstock/).

## License
This project is made available under the [Apache 2.0 License](/LICENSE).
