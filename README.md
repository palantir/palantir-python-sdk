# Palantir Foundry SDK
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/palantir-sdk)
[![PyPI](https://img.shields.io/pypi/v/palantir-sdk)](https://pypi.org/project/palantir-sdk/)
[![License](https://img.shields.io/badge/License-Apache%202.0-lightgrey.svg)](https://opensource.org/licenses/Apache-2.0)
[![Autorelease](https://img.shields.io/badge/Perform%20an-Autorelease-success.svg)](https://autorelease.general.dmz.palantir.tech/palantir/palantir-python-sdk)

This SDK is incubating and subject to change.

## Quickstart

Instantiate a FoundryClient to access operations in the SDK.

Configuration for hostname and an authentication token are provided by environment variables (`FOUNDRY_HOSTNAME`, `FOUNDRY_TOKEN`)

* `FOUNDRY_HOSTNAME` is the hostname of your instance e.g. `example.palantirfoundry.com`
* `FOUNDRY_TOKEN` is a token acquired from the `Tokens` section of Foundry Settings

Authentication tokens serve as a private password and allows a connection to Foundry data. Keep your token secret and do not share it with anyone else. Do not add a token to a source controlled or shared file.


```python
from foundry import FoundryClient

client = FoundryClient()

my_dataset = client.datasets.Dataset.get(...)
```

You can alternatively pass in the hostname and token as keyword arguments when initializing the FoundryClient.


## Examples

### Read a Foundry Dataset as a table
```python
Dataset = client.datasets.Dataset

# Create dataset
my_dataset = Dataset.create(name="...", parent_folder_rid="...")

# Get dataset
my_dataset = Dataset.get(dataset_rid="...")

# Read dataset as table
with open("my_table.csv", "wb") as f:
    f.write(Dataset.read_table(dataset_rid="...", format="CSV", columns=[...]))
```


### Manipulate a Dataset within a Transaction
```python
my_transaction = client.datasets.Transaction.create(dataset_rid="...")

with open("my/path/to/file.txt", 'rb') as f:
    client.datasets.File.upload(data=f.read(), dataset_rid="....", file_path="...")

client.datasets.Transaction.commit(dataset_rid="...", transaction_rid=my_transaction.rid)
```

### Read the contents of a file from a dataset (by exploration / listing)
```python
files = list(client.datasets.File.iterate(dataset_rid="..."))

file_path = files[0].path

client.datasets.File.get_content(dataset_rid = "...", file_path=file_path).read()
```
```
b'Hello!'
```

### Read a Foundry Dataset into a Pandas DataFrame
```python
import pyarrow as pa

stream = client.datasets.Dataset.read_table(dataset_rid="...")
df = pa.ipc.open_stream(stream).read_all().to_pandas()
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


## Contributing

See the [CONTRIBUTING.md](./CONTRIBUTING.md) document.  Releases are published to [pypi](https://pypi.org/project/palantir-sdk/) on tag builds and are automatically re-published to [conda](https://anaconda.org/conda-forge/palantir-sdk) using [conda-forge](https://github.com/conda-forge/palantir-sdk-feedstock/).

## License
This project is made available under the [Apache 2.0 License](/LICENSE).
