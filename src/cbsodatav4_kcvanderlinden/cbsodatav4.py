import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import os
import sqlite3
import sys

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
    while retry_count < 5:
        try:
            response = session.get(target_url)
            if response.status_code == 200:
                return response.json()
            else:
                raise requests.exceptions.RequestException(f"HTTP {response.status_code}")
        except requests.exceptions.ChunkedEncodingError:
            print(f"ChunkedEncodingError occurred. Retrying...")
            retry_count += 1
            if retry_count == 5:
                raise requests.exceptions.ChunkedEncodingError("Max retries exceeded")

def getData(target_url:str) -> pd.DataFrame:
    '''
    Get the data from the target URL.
    target_url: str, the URL to get the data from
    tableLength: int, the number of rows in the table
    '''       
    conn = sqlite3.connect(':memory:')
    pages_read = 0
    nextPage = True
    while nextPage: 
        response = cbsConnect(target_url)
        nextPage = True if "@odata.nextLink" in response else False
        df = pd.DataFrame(response['value']) 
        if not nextPage and pages_read == 0:
            return df
        else:
            cacheTable(df, 'loop_data', conn, pages_read > 0)
            pages_read += 1
            nextPage = True if "@odata.nextLink" in response else False
            target_url = response["@odata.nextLink"] if nextPage else None
    df = pd.read_sql('SELECT * FROM loop_data', conn)
    return df

def cacheTable(df: pd.DataFrame, tableName: str, conn, tableCreated: bool = False):
    if tableCreated == False:
        c = conn.cursor()
        columns = df.columns.tolist()
        create_table_query = f"CREATE TABLE {tableName} ({', '.join([f'{col} TEXT' for col in columns])});"
        c.execute(create_table_query)
    df.to_sql(tableName, conn, if_exists='append', index=False)

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

    if name == None:
        df = fullDataset(tableID, limit, dataFilter, customFilter)
    else:
        df = pd.DataFrame(specificTable(tableID, name, limit, dataFilter, customFilter))

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
        df_type = df = pd.DataFrame(specificTable(tableID, typefilter))
        dataFilterValues = df_type.loc[df_type["Identifier"].str.contains(filtervalue), "Identifier"].values
        dataFilterlist = [f"RegioS eq '{val}'" for val in dataFilterValues]
        conn = sqlite3.connect(':memory:')
        for i, dataFilter in enumerate(dataFilterlist):
            statusPrint(len(dataFilterlist), i)
            df = getData(targetUrl(tableID, "Observations", limit, dataFilter))
            tableCreated = True if i > 0 else False
            cacheTable(df, 'data_table', conn, tableCreated)
        df = pd.read_sql('SELECT * FROM data_table', conn)
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
            df = pd.merge(df, dimTable, on=column.replace("Codes", ""))
            df = df.rename(columns={"Title": f"{column}Title"})
        elif column.endswith("Groups"): # Groups are used to create a hierarchy
            parents_list = []
            for index, row in dimTable.iterrows():
                parents = get_parents(row, dimTable)
                parents_list.append(parents)
            dimTable[f'{column.replace("Groups", "")}parents'] = parents_list
            dimTable.rename(columns={'Id':f'{column}MergeId'}, inplace=True)
            columndfMerge = [col for col in df.columns if col.lower().endswith('id') and col.startswith(column.replace("Groups", ""))][0]
            df = pd.merge(df, dimTable, left_on=columndfMerge, right_on=f'{column}MergeId', how='left')
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
        df = getData(targetUrl(tableID, name, limit, dataFilter), tableLengthObservations(tableID))
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
    sys.stdout.write('\r')
    sys.stdout.write(f"[%-{totalAmount}s] %d%%" % ('='*(currentAmount+1), currentAmount/totalAmount*100))
    sys.stdout.flush()