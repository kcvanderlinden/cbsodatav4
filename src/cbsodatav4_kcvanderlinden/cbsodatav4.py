import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import os
import sqlite3
import sys
import concurrent.futures
import json
import dask.dataframe as dd

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
    while True:
        try:
            response = session.get(target_url)
            if response.status_code == 200:
                # for chunk in response.iter_lines(decode_unicode=True):
                #     row = json.loads(chunk)
                #     data.append(row)
                #  # Raise an exception for bad status codes
                data = response.json()  # Load the JSON data directly from the response object
            else:
                raise requests.exceptions.RequestException(f"HTTP {response.status_code}")
        except requests.exceptions.ChunkedEncodingError:
            print(f"ChunkedEncodingError occurred. Retrying...")
            retry_count += 1
            if retry_count == 5:
                raise requests.exceptions.ChunkedEncodingError("Max retries exceeded")
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}. Retrying...")
        else:
            break
    return data

def getData(target_url:str) -> pd.DataFrame:
    '''
    Get the data from the target URL.
    target_url: str, the URL to get the data from
    tableLength: int, the number of rows in the table
    '''
    # conn = sqlite3.connect('./temp.db')
    pages_read = 0
    nextPage = True
    while nextPage:
        response = cbsConnect(target_url)
        nextPage = True if "@odata.nextLink" in response else False
        df = dd.from_pandas(pd.DataFrame(response['value']), npartitions=1)
        df = df.compute() # Compute the dataframe to trigger the computation
        # df = dd_df.drop_duplicates().compute()
        if not nextPage and pages_read == 0:
            return df
        else:
            cacheTable(df, 'loop_data', 'sqlite:///./temp.db', pages_read > 0)
            pages_read += 1
            nextPage = True if "@odata.nextLink" in response else False
            target_url = response["@odata.nextLink"] if nextPage else None
    # df = pd.read_sql('SELECT * FROM loop_data', conn)
    df = dd.read_sql_table('loop_data', 'sqlite:///./temp.db', index_col='Id')
    df = df.reset_index()
    # Add an index with the Dask `index` method
    # df = df.compute()
    
    # df = dd_df.compute()
    
    return df

def cacheTable(df, tableName: str, conn, tableCreated: bool = False):
    # if tableCreated == False:
    #     c = conn.cursor()
    #     # columns = df.columns.tolist()
    #     # create_table_query = f"CREATE TABLE {tableName} ({', '.join([f'{col} TEXT' for col in columns])});"
    #     #c.execute(create_table_query)
    df.to_sql(tableName, 'sqlite:///./temp.db', if_exists='append', index=False)
    

def DataFrame(tableID:str, name:str=None, limit:int=None, dataFilter:str=None, customFilter:str=None, cache:bool=False) -> pd.DataFrame:
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
    cache : bool, optional
        Whether or not to cache the DataFrame, by default False.

    Returns
    -------
    Pandas DataFrame
        A DataFrame containing the retrieved data.
    """
    
    tablename = tableName(tableID, name, dataFilter, limit)

    # check for cached version if cache is True
    if cache:
        try:
            df = pd.read_csv(f"./cache/{tablename}.csv")
            return df
        except:
            pass
    # get data
    conn = sqlite3.connect('./temp.db')
    conn.close()
    if name == None:
        df = fullDataset(tableID, limit, dataFilter, customFilter)
    else:
        df = specificTable(tableID, name, limit, dataFilter, customFilter)
    # remove temp.db file
    try:
        os.remove('./temp.db')
    except FileNotFoundError:
        pass
    # if cache is true, check if cache folder exists, otherwise create it
    if cache:
        if not os.path.exists("./cache"):
            os.makedirs("./cache")
        df.to_csv(f"./cache/{tablename}.csv", index=False)
    return df

def fullDataset(tableID:str, limit:int=None, dataFilter:str=None, customFilter:str=None) -> pd.DataFrame:
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
        dataFilterValues = df_type.loc[df_type["Identifier"].str.contains(filtervalue), "Identifier"].values
        dataFilterlist = [f"RegioS eq '{val}'" for val in dataFilterValues]
        # conn = sqlite3.connect('./temp.db')
        
        sys.stdout.write(f'Totaal aantal soorten op basis van filter {len(dataFilterlist)}')
        sys.stdout.write('\n')
        dfs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(getData, targetUrl(tableID, "Observations", limit, filter)): i for i, filter in enumerate(dataFilterlist)}
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                statusPrint(len(dataFilterlist), i)
                # try:
                df = future.result()
                # Append each dataframe to the list and then use dask's `compute` method to combine them into a single dataframe
                dfs.append(df)
                # except Exception as e:
                #     print(f"Error processing filter {i+1}: {e}")
                
                tableCreated = True if i > 0 else False

                # cacheTable(df, 'data_table', 'sqlite:///./temp.db', tableCreated)
                # except Exception as e:
                #     print(f"Error processing filter {i}: {e}")
            
        df = dd.concat(dfs)
            
        # df = dd.read_sql_table('data_table', 'sqlite:///./temp.db', index_col='Id')
        # df = df.compute()
    else:
        df = getData(targetUrl(tableID, "Observations", limit, dataFilter))

    availableDimTables = [df['name'] for df in requests.get(f"{basic_odata_url}{tableID}").json()['value']]
    codeDimTables = [dtable for dtable in availableDimTables if dtable.endswith("Codes")]
    groupDimTables = [dtable for dtable in availableDimTables if dtable.endswith("Groups") and dtable not in ['PeriodenGroups', 'RegioSGroups']]

    for column in codeDimTables+groupDimTables:
        try:
            dimTable = getData(targetUrl(tableID, column))
        except:
            continue
        dimTable = dimTable.drop(columns=["Index", "Description"])
        if column.endswith("Codes"): # Codes are used to map to a specific dimensions
            for col in dimTable.columns:
                if col.lower().endswith('id'):
                    dimTable.rename(columns={col:f'{column.replace("Codes", "")}Id'}, inplace=True)
            dimTable.rename(columns={"Identifier": column.replace("Codes", "")}, inplace=True)
            df = df.merge(dimTable, on=column.replace("Codes", ""))
            df = df.rename(columns={"Title": f"{column}Title"})
        elif column.endswith("Groups"): # Groups are used to create a hierarchy
            parents_list = []
            for index, row in dimTable.iterrows():
                parents = get_parents(row, dimTable)
                parents_list.append(parents)
            dimTable[f'{column.replace("Groups", "")}parents'] = parents_list
            dimTable.rename(columns={'Id':f'{column}MergeId'}, inplace=True)
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
    tableID: str, the table ID of the dataset
    name: str, the name of the table to retrieve
    limit: int, the maximum number of rows to retrieve
    dataFilter: str, the filter to apply to the data
    '''
    if name != "/Observations":
        df = getData(targetUrl(tableID, name, limit, dataFilter))
    else:
        df = getData(targetUrl(tableID, name, limit, dataFilter)) # , tableLengthObservations(tableID))
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

def get_parents(row:str, df: pd.DataFrame):
    '''
    this function returns the titles of all parents of a row in a dataframe.
    row: str, the id of a row in a dataframe
    df: pd.DataFrame, the dataframe to get the row from, and also used as a cache for already processed rows
    '''
    parent = row['ParentId']
    parentTitles = []
    while parent is not None:
        p = df[df['Id'] == parent]
        if not p.empty:
            parentTitles.append(p.iloc[0]['Title'])
            parent = p.iloc[0]['ParentId']
        else:
            break
    return '|'.join(parentTitles[::-1])

def statusPrint(totalAmount:int, currentAmount:int):
    max_width_bar = 30
    width_bar = totalAmount if totalAmount <= max_width_bar else max_width_bar
    progress_in_bar = totalAmount / width_bar
    sys.stdout.write('\r')
    sys.stdout.write(f"[%-{progress_in_bar}s] %d%%" % ('='*(currentAmount+1), (currentAmount+1)/totalAmount*100))
    sys.stdout.flush()