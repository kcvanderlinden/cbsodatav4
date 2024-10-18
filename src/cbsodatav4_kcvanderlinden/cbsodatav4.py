import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import os
import sys
import concurrent.futures
import json
import dask.dataframe as dd
import dask
import random
from time import sleep
from dask.distributed import LocalCluster
cluster = LocalCluster(processes=True, # notice this line
                           n_workers=1,
                           memory_limit="2 GiB")

basic_odata_url = 'https://datasets.cbs.nl/odata/v1/CBS/'

def cbsConnect(target_url:str):
    '''
    This function connects to the CBS API and returns a list of dictionaries.
    target_url: str, the URL is the url that contains the data you want to retrieve
    returns: list
    '''
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 500, 502, 503, 504 ])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    retry_count = 0
    data = []
    time_out_time = 0
    while True:
        try:
            response = session.get(target_url)
            if response.status_code == 200:
                data = response.json()  # Load the JSON data directly from the response object
                return data
            else:
                raise requests.exceptions.RequestException(f"HTTP {response.status_code}")
        except requests.exceptions.ChunkedEncodingError:
            print(f"ChunkedEncodingError occurred (data load incomplete). Retrying...")
            retry_count += 1
            time_out_time += random.randrange(1, 10)/10 # dynamic time-out after unstable connection with host.
            sleep(time_out_time)
            if retry_count == 5:
                raise requests.exceptions.ChunkedEncodingError("Max retries exceeded")
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}. Retrying...")
        else:
            break

def getData(target_url:str, return_data = False):
    '''
    Get the data from the target URL.
    target_url: str, the URL to get the data from
    tableLength: int, the number of rows in the table
    '''
    pages_read = 0
    nextPage = True
    dicts = []
    hex_target_url = ''.join(format(ord(c), 'x') for c in target_url)[-20:]
    while nextPage:
        response = cbsConnect(target_url)
        nextPage = True if "@odata.nextLink" in response else False
        if not nextPage and pages_read == 0:
            dicts += response['value']
        else:
            dicts += response['value']
            pages_read += 1
            nextPage = True if "@odata.nextLink" in response else False
            target_url = response["@odata.nextLink"] if nextPage else target_url
    if return_data:
        return response['value']
    else:
        if not os.path.exists("./temporary_json"):
            os.makedirs("./temporary_json")
        # write every page to json is easier on the system memory and a little more demanding on the disk space
        with open(f'./temporary_json/{hex_target_url}.json', 'w', encoding='utf-8') as f:
            json.dump(dicts, f, ensure_ascii=False, indent=4)

def DataFrame(tableID:str, name:str=None, limit:int=None, dataFilter:str=None, customFilter:str=None, save_csv_dir:str=None):
    """
    Return a Pandas DataFrame containing the data from the CBS OData API.

    Parameters
    ----------
    tableID : str
        The ID of the table to retrieve data from.
    name : str, optional
        The name of the specific table to retrieve, by default None.
        If not specified, the entire dataset will be retrieved.
    limit : int, optional
        The maximum number of rows to retrieve, by default None.
        If not specified, all rows will be retrieved.
    dataFilter : str, optional
        A filter to apply to the data, by default None.
        If not specified, no filter will be applied.
    save_csv_dir : str, optional
        if not None, save the csv file to the specified directory.

    Returns
    -------
    Pandas DataFrame
        A DataFrame containing the retrieved data.
    """
    
    tablename = tableName(tableID, name, dataFilter, limit)

    if name == None:
        df = fullDataset(tableID, limit, dataFilter, customFilter)
    else:
        df = specificTable(tableID, name, limit, dataFilter, customFilter)
    # if cache is true, check if cache folder exists, otherwise create it
    if save_csv_dir is not None:
        df.to_csv(f"{save_csv_dir}/{tablename}.csv", index=False, single_file=True)
    df = df.compute()
    # delete_json_files()
    return df

def fullDataset(tableID:str, limit:int=None, dataFilter:str=None, customFilter:str=None):
    '''
    Get the full dataset of a table, including all codes
    tableID: str, the table ID of the dataset
    limit: int, the maximum number of rows to retrieve
    dataFilter: str, the filter to apply to the data
    '''
    if customFilter is not None:
        typefilter = customFilter['type']
        filtervalue = customFilter['filterValue']
        df_type = specificTable(tableID, typefilter)
        df_type = dd.from_pandas(pd.DataFrame(df_type), npartitions=1)
        dataFilterValues = df_type.loc[df_type["Identifier"].str.contains(filtervalue), "Identifier"].values.compute()
        
        dataFilterlist = [f"RegioS eq '{val}'" for val in dataFilterValues]
        sys.stdout.write(f'Totaal aantal soorten op basis van filter {len(dataFilterlist)}')
        sys.stdout.write('\n')
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(getData, targetUrl(tableID, "Observations", limit, filter)): i for i, filter in enumerate(dataFilterlist)}
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                statusPrint(len(dataFilterlist), i)
                try:
                    
                    # Append each dataframe to the list and then use dask's `compute` method to combine them into a single dataframe
                    future.result()
                except Exception as e:
                    print(f"Error processing filter {i+1}: {e}")

        df = dd.read_json(
            get_json_files(), 
            blocksize=None, orient="records", 
            lines=False
            ) 
    else:
        df = getData(targetUrl(tableID, "Observations", limit, dataFilter))

    availableDimTables = [df['name'] for df in requests.get(f"{basic_odata_url}{tableID}").json()['value']]
    codeDimTables = [dtable for dtable in availableDimTables if dtable.endswith("Codes") and dtable not in ['RegioSCodes']]
    groupDimTables = [dtable for dtable in availableDimTables if dtable.endswith("Groups") and dtable not in ['PeriodenGroups', 'RegioSGroups']]

    for column in codeDimTables+groupDimTables:
        try:
            dimTable = getData(targetUrl(tableID, column), return_data=True)
            dimTable = dd.from_pandas(pd.DataFrame(dimTable), npartitions=1)
            
        except:
            continue
        dimTable = dimTable.drop(columns=["Index", "Description"])
        if column.endswith("Codes"): # Codes are used to map to a specific dimensions
            merge_key = column.replace("Codes", "")
            for col in dimTable.columns:
                if col.lower().endswith('id'):
                    dimTable = dimTable.rename(columns={col:f'{merge_key}Id'})
            dimTable = dimTable.rename(columns={"Identifier": merge_key})
            df = df.astype({
                merge_key : 'string[pyarrow]'
            })
            df = df.merge(dimTable, on=merge_key, how='left')
            df = df.rename(columns={"Title": f"{column}Title"})
        elif column.endswith("Groups"): # Groups are used to create a hierarchy
            parents_list = []
            for index, row in dimTable.iterrows():
                parents = get_parents(row, dimTable)
                parents_list.append(parents)
            dimTable[f'{column.replace("Groups", "")}parents'] = dask.array.from_array(parents_list)
            dimTable = dimTable.rename(columns={'Id':f'{column}MergeId'})
            columndfMerge = [col for col in df.columns if col.lower().endswith('id') and col.startswith(column.replace("Groups", ""))][0]
            df = df.merge(dimTable, left_on=columndfMerge, right_on=f'{column}MergeId', how='left')
            df = df.rename(columns={"Title": f"{column}Title"})
        if column.startswith("Wijken"):
            df = df.rename(columns={column: "RegioCode"})
            df["GemeenteCode"] = df["DimensionGroupId"]
            df['RegioCode'] = df['WijkenEnBuurten']
        if column.lower().endswith("regioscodes"):
            df[column] = df[column.replace("Codes", "")].copy()
    standardColumns =  ['Id','Value', 'ValueAttribute', 'StringValue','RegioS', 'Parents', 'GemeenteCode', 'RegioCode']
    columnsToKeep = [col for col in df.columns if col in standardColumns]
    columnsToKeep += [col for col in df.columns if col.endswith('Title')]
    columnsToKeep += [col for col in df.columns if col.endswith('parents')]
    df = df[columnsToKeep]

    # replace 'Codes','Groups' and 'Title' in columnnames
    df.columns = [col.replace('CodesTitle', '').replace("GroupsTitle", "Category") for col in df.columns]

    # lowercase all columnnames
    df.columns  = [col.lower() for col in df.columns]

    

    return df

def specificTable(tableID:str, name:str, limit:int=None, dataFilter:str=None):
    '''
    Get a specific table from the dataset.
    tableID: str, the table ID of the dataset
    name: str, the name of the table to retrieve
    limit: int, the maximum number of rows to retrieve
    dataFilter: str, the filter to apply to the data
    '''
    if name != "/Observations":
        df = getData(targetUrl(tableID, name, limit, dataFilter), return_data=True)
    else:
        df = getData(targetUrl(tableID, name, limit, dataFilter), return_data=True) # , tableLengthObservations(tableID))
    return df

def tableName(tableID:str, name:str, dataFilter:str, limit:int):
    '''
    Create a tablename based on the dataFilter and tableID.
    dataFilter: str, the filter to apply to the data
    tableID: str, the table ID of the dataset
    '''
    tablename = f"{tableID}"
    if name!=None:
        tablename += f"_{name}"
    elif name==None:
        tablename += "_allTables"
    if dataFilter!=None:
        filter_as_string = dataFilter.replace(" eq", "").replace("'", "").replace('"', "").replace(' ', '_').strip() # TODO as one regex
        tablename += "_" + filter_as_string 
    if limit!=None:
        tablename += f"_limit={limit}"
    return tablename

def tableLengthObservations(tableID:str):
    '''
    Get the number of rows in the observations table.
    tableID: str, the table ID of the dataset
    '''
    observationCount = requests.get(f"{basic_odata_url}{tableID}/Properties").json()['ObservationCount']
    return observationCount

def targetUrl(tableID:str, name:str, limit:int=None, dataFilter:str=None):
    '''
    Create the target URL for the API request.
    tableID: str, the table ID of the dataset
    name: str, the name of the table to retrieve
    limit: int, the maximum number of rows to retrieve
    dataFilter: str, the filter to apply to the data
    '''
    tableUrl = f"{basic_odata_url}{tableID}"
    target_url = f"{tableUrl}/{name}"
    if limit != None:
        target_url += f"?$top={limit}"
        target_url += f"&" if dataFilter != None else ""
    if dataFilter != None:
        target_url += f"?$filter={dataFilter}"

    return target_url

def get_parents(row:str, df:dd):
    '''
    this function returns the titles of all parents of a row in a dataframe.
    row: str, the id of a row in a dataframe
    df: pd.DataFrame, the dataframe to get the row from, and also used as a cache for already processed rows
    '''
    parent = row['ParentId']
    parentTitles = []
    while parent is not None:
        p = df[df['Id'] == parent].reset_index()
        if p.index.compute().stop > 0:
            parentTitles.append(p['Title'].compute()[0])
            parent = p['ParentId'].compute()[0]
        else:
            break
    return '|'.join(parentTitles[::-1])

def statusPrint(totalAmount:int, currentAmount:int):
    current_percentage = int((currentAmount +1) / totalAmount * 100)
    current_bar = int((currentAmount +1) / totalAmount * 10)
    sys.stdout.write('\r')
    sys.stdout.write(f"[%-{10}s] %d%%" % ('='*(current_bar), current_percentage))
    sys.stdout.flush()

def get_json_files():
    '''
    Get a list of all .json files in the ./cache folder.
    '''
    json_files = []
    for filename in os.listdir("./temporary_json"):
        if filename.endswith(".json"):
            json_files.append(f'./temporary_json/{filename}')
    return json_files

def delete_json_files():
    '''
    Delete all .json files in the ./cache folder.
    '''
    for filename in os.listdir("./temporary_json"):
        if filename.endswith(".json"):
            os.remove(os.path.join("./temporary_json", filename))
    print("All .json files deleted from ./cache folder")
