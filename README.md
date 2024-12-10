# Readme

This is a Python application that retrieves data from the CBS OData API and converts it into a Pandas DataFrame. The application allows users to retrieve specific tables or the entire dataset, and apply filters to the data.

## Usage
To use this application, follow these steps:
1. Install the package using the following pip command: `pip install git+https://github.com/kcvanderlinden/cbsodatav4.git`.
2. Create a new Python file and import the necessary libraries:
```python
import pandas as pd
from cbsodatav4_kcvanderlinden.cbsodatav4 import DataFrame
```
3. Use the `DataFrame` function to retrieve data from the CBS OData API:
```python
df = DataFrame(tableID, name=None, limit=None, dataFilter=None, cache=False)
```
The following parameters can be specified when calling Dataframe:
```Parameters
    ----------
    tableID : str
        The ID of the table to retrieve data from.
    name : str, optional
        The name of the specific table of the source to retrieve, by default None.
        If not specified, all tables will be retrieved and merged into one table.
    limit : int, optional
        The maximum number of rows to retrieve, by default None.
        If not specified, all rows will be retrieved.
    dataFilter : str, optional
        A filter to apply to the data, by default None.
        If not specified, no filter will be applied.
    save_csv_path : str, optional
        if not None, save the csv file to the specified directory. Path must incude the filename.
``` 
4. Once the DataFrame has been retrieved, you can use it for further analysis or exporting the data to a file.

## Example Usage
Here is an example of how to retrieve the entire dataset from table ID "84916NED" and apply a filter:
```python
df = DataFrame('82213NED', dataFilter={'RegioS': 'GM'}, limit=5)
print(df.head())
```
This will retrieve the first 5 rows of the dataset from table ID "82213NED" and filters on the varibale RegioS on values that contain/startwith 'GM'.

## Filtering explanation
It is supported to filter using ODATA4 logic: filter in column on stringvalues containing certain characters. This allows for filtering on for example all RegioS that start with 'GM' (only municipality codes) or Perioden that contain '202' which results on rows that are 2020 or higher or that contain 'JJ' which retrieves rows only on whole years. All variables (excluding id and value) that are visible in the [/Observations](https://datasets.cbs.nl/odata/v1/CBS/82213NED/Observations) view can be filtered on the exact value or as partially matched (containing a value).

## License
This project is licensed under the MIT License - see the [LICENSE](https://github.com/[USERNAME]/[REPO_NAME]/blob/master/LICENSE) file for details.
