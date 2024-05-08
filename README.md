# Readme

This is a Python application that retrieves data from the CBS OData API and converts it into a Pandas DataFrame. The application allows users to retrieve specific tables or the entire dataset, and apply filters to the data.

## Usage
To use this application, follow these steps:
1. Install the package using the following pip command: `pip install git+https://github.com/kcvanderlinden/cbsodatav4.git`.
2. Install the required packages by running another pip command `pip install -r https://raw.githubusercontent.com/kcvanderlinden/cbsodatav4/main/requirements.txt`.
3. Create a new Python file and import the necessary libraries:
```python
import pandas as pd
from cbsodatav4_kcvanderlinden.cbsodatav4 import DataFrame
```
4. Use the `DataFrame` function to retrieve data from the CBS OData API:
```python
df = DataFrame(tableID, name=None, limit=None, dataFilter=None, cache=False)
```
The `tableID` parameter specifies the ID of the table to retrieve data from. The `name` parameter allows users to specify a specific table to retrieve, and is set to `None` by default. The `limit` parameter sets the maximum number of rows to retrieve, and is also set to `None` by default. The `dataFilter` parameter specifies a filter to apply to the data, and is also set to `None` by default. Finally, the `cache` parameter allows users to specify whether or not to cache the DataFrame, and is set to `False` by default.
5. Once the DataFrame has been retrieved, you can use it for further analysis or exporting the data to a file.

## Example Usage
Here is an example of how to retrieve the entire dataset from table ID "84916NED" and apply a filter:
```python
df = DataFrame("84916NED", limit=5)
print(df)
```
This will retrieve the first 5 rows of the dataset from table ID "84916NED".

## License
This project is licensed under the MIT License - see the [LICENSE](https://github.com/[USERNAME]/[REPO_NAME]/blob/master/LICENSE) file for details.
