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

basic_odata_url = 'https://datasets.cbs.nl/odata/v1/CBS/'

def DataFrame(tableID:str, name:str=None, limit:int=None, dataFilter:str=None, save_csv_path:str=None):
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
    save_csv_path : str, optional
        if not None, save the csv file to the specified directory. Path must incude the filename.

    Returns
    -------
    Pandas DataFrame
        A DataFrame containing the retrieved data.
    """
    
    if name == None:
        df = fullDataset(tableID, limit, dataFilter)
    else:
        df = specificTable(tableID, name, limit, dataFilter)
    # if cache is true, check if cache folder exists, otherwise create it
    if save_csv_path is not None:
        df.to_csv(f"{save_csv_path}.csv", index=False, single_file=True)
        sys.stdout.write(f'\nwritten table to csv')
    df = df.compute()
    delete_json_files()
    return df

def cbsConnect(target_url:str):
    '''
    This function connects to the CBS API and returns a list of dictionaries.
    
    Parameters
    ----------
    target_url : str
        The URL of the data you want to retrieve.
    Returns
    -------
    list
        A list of dictionaries containing the retrieved data.
    
    Examples
    --------
    >>> cbsConnect('https://datasets.cbs.nl/odata/v1/CBS/$metadata')
    '''
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 500, 502, 503, 504 ])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    retry_count = 0
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
            sys.stdout.write(f"ChunkedEncodingError occurred (data load incomplete). Retrying...")
            retry_count += 1
            time_out_time += random.randrange(1, 10)/10 # dynamic time-out after unstable connection with host.
            sleep(time_out_time)
            if retry_count == 5:
                raise requests.exceptions.ChunkedEncodingError("Max retries exceeded")
        except json.JSONDecodeError as e:
            sys.stdout.write(f"JSON decode error: {e}. Retrying...")

def getData(target_url:str, return_data:bool = False):
    '''
    Get the data from the target URL.

    Parameters
    ----------
    target_url : str
        The URL to retrieve data from.
    return_data : bool, optional
        If True, return the retrieved data directly. Otherwise, return None (default is False).
    
    Returns
    -------
    dict or None
        A dictionary containing the retrieved data if return_data=True; otherwise, None.
    '''
    hex_target_url = ''.join(format(ord(c), 'x') for c in target_url)[-20:]
    response = cbsConnect(target_url)
    if return_data:
        return response['value']
    else:
        writeJson(response['value'], hex_target_url)

def fullDataset(tableID:str, limit:int=None, dataFilter:str=None):
    """
    Get the full dataset of a table, including all codes.

    Parameters
    ----------
    tableID : str
        The ID of the table to retrieve data from.
    limit : int, optional
        The maximum number of rows to retrieve, by default None.
        If not specified, all rows will be retrieved.
    dataFilter : str, optional
        A filter to apply to the data, by default None.
        If not specified, no filter will be applied.

    Returns
    -------
    Pandas DataFrame
        A DataFrame containing the retrieved data.
    """
    urlOfObservations = targetUrl(tableID, "Observations", limit, dataFilter)
    numberOfObservationsUrl = f'{urlOfObservations}&$count=true&$top=0'
    numberOfObservations = cbsConnect(numberOfObservationsUrl)['@odata.count']
    if numberOfObservations == 0:
        raise Exception("The number of observations is equal to 0. Maybe a filter is incorrect.")
    listOfObservationUrls = listOfUrls(urlOfObservations, numberOfObservations)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(getData, observationUrl): i for i, observationUrl in enumerate(listOfObservationUrls)}
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            statusPrint(numberOfPages(numberOfObservations), i)
            try:
                future.result()
            except Exception as e:
                sys.stdout.write(f"\nError processing filter {i+1}: {e}")
    df = dd.read_json(
        get_json_files(), 
        blocksize=None, orient="records", 
        lines=False
        )

    availableDimTables = [df['name'] for df in requests.get(f"{basic_odata_url}{tableID}").json()['value']]
    codeDimTables = [dtable for dtable in availableDimTables if dtable.endswith("Codes")]
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
    standardColumns =  ['Id','Value', 'ValueAttribute', 'StringValue','RegioSCodes', 'Parents', 'GemeenteCode', 'RegioCode']
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
    
    Parameters
    ----------
    tableID : str
        The ID of the table to retrieve.
        
    name : str
        The name of the table to retrieve.
        
    limit : int, optional
        The maximum number of rows to retrieve. Defaults to None.
        
    dataFilter : str, optional
        The filter to apply to the data. Defaults to None.

    Returns
    ------
    df : dask DataFrame
        The retrieved table as a Dask DataFrame.
    '''
    if name != "/Observations":
        df = getData(targetUrl(tableID, name, limit, dataFilter), return_data=True)
    else:
        df = getData(targetUrl(tableID, name, limit, dataFilter), return_data=True)
    return df

def targetUrl(tableID:str, name:str, limit:int=None, dataFilter:str=None):
    """
    Create the target URL for the API request.
    
    Parameters
   -----------
    tableID : str
        The ID of the table to retrieve.
        
    name : str
        The name of the table to retrieve. If None, retrieves all tables.
        
    limit : int, optional
        The maximum number of rows to retrieve (default=None).
        
    dataFilter : str, optional
        A dictionary containing filter parameters (default=None).
    
    Returns
   -----------
    target_url : str
        The constructed target URL for the API request.
    """
    tableUrl = f"{basic_odata_url}{tableID}"
    target_url = f"{tableUrl}/{name}?"
    if limit != None:
        target_url += f"?$top={limit}"
        target_url += f"&" if dataFilter != None else ""
    if dataFilter != None:
        for i, k in enumerate(dataFilter):
            target_url += f"$filter=" if i == 0 else "%20and%20"
            target_url += f"contains({k},%27{dataFilter[k]}%27)"
    return target_url

def numberOfPages(limit:int):
    """
    Calculate the number of pages required to process a large dataset.

    Parameters:
        limit (int): The maximum number of rows per page.

    Returns:
        int: The total number of pages.
    """
    pageEntryLimit = 100000
    numberOfRequiredPages = -(-limit // pageEntryLimit)
    return numberOfRequiredPages

def listOfUrls(url:str, limit:int=None):
    """
    Generate a list of URLs for the given URL and optional limit.

    Args:
        url (str): The base URL.
        limit (int, optional): The maximum number of rows to retrieve. Defaults to None.

    Returns:
        list: A list of URLs.
    """
    return [f'{url}&$skip={x*100000}' for x in range(numberOfPages(limit))]

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
    """
    Print a status message indicating the progress of processing a large dataset.

    Parameters:
        totalAmount (int): The total number of rows in the dataset.
        currentAmount (int): The current number of rows being processed.

    Returns:
        None
    """
    """
    This function prints a status message to indicate the progress of processing a large dataset.
    
    Parameters:
        totalAmount (int): The total number of rows in the dataset.
        currentAmount (int): The current number of rows being processed.

    Returns:
        None
    """
    current_percentage = int((currentAmount +1) / totalAmount * 100)
    current_bar = int((currentAmount +1) / totalAmount * 10)
    sys.stdout.write('\r')
    sys.stdout.write(f"[%-{10}s] %d%%" % ('='*(current_bar), current_percentage))
    sys.stdout.flush()

def writeJson(data:list, filename:str):
    """
    Write JSON data to a file.

    Parameters
    ----------
    data : list
        The data to be written as JSON.
    filename : str
        The name of the file where the JSON data will be written.

    Returns
    -------
    None

    Note: This function assumes that the input data is a Python list, and will write it to a file in the current working directory. If you want to write the file to a different location or have specific formatting requirements, this function may need to be modified.
    """
    if not os.path.exists("./temporary_json"):
        os.makedirs("./temporary_json")
    # write every page to json is easier on the system memory and a little more demanding on the disk space
    with open(os.path.join('./temporary_json/', f'{filename}.json'), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def get_json_files():
    """
    This function generates a list of JSON file names based on the provided data.
    
    Parameters:
        None
        
    Returns:
        list: A list of JSON file names.
    """
    
    json_files = []
    for filename in os.listdir("./temporary_json"):
        if filename.endswith(".json"):
            json_files.append(os.path.join("./temporary_json", filename))
    return json_files

def delete_json_files():
    """
    This function deletes all JSON files from the ./temporary_json directory.

    Parameters:
        None

    Returns:
        None
    """
    if os.path.exists("./temporary_json"):    
        for filename in os.listdir("./temporary_json"):
            if filename.endswith(".json"):
                os.remove(os.path.join("./temporary_json", filename))
        # Remove the empty directory after deleting all files
        os.rmdir("./temporary_json")
