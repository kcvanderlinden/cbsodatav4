import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import concurrent.futures
import os
import math

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
                return response.json()['value']
            else:
                raise requests.exceptions.RequestException(f"HTTP {response.status_code}")
        except requests.exceptions.ChunkedEncodingError:
            print(f"ChunkedEncodingError occurred. Retrying...")
            retry_count += 1
            if retry_count == 5:
                raise requests.exceptions.ChunkedEncodingError("Max retries exceeded")

def getData(target_url:str, tableLength:int=0):
    '''
    Get the data from the target URL.
    target_url: str, the URL to get the data from
    tableLength: int, the number of rows in the table
    '''       
    if tableLength > 100000:
        queryAmount = int(math.ceil(tableLength / 100000))
        queryJobs = [f"{target_url}?$skip={i*100000}" for i in range(queryAmount)]
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(cbsConnect, job) for job in queryJobs]
            concurrent.futures.wait(futures)
            listOfListOfDicts = [future.result() for future in futures]
            listOfDicts = [item for sublist in listOfListOfDicts for item in sublist]
            df = pd.DataFrame(listOfDicts)
    else:
        r = requests.get(target_url).json()
        df = pd.DataFrame(r['value'])
    
    return df

def DataFrame(tableID:str, name:str=None, limit:int=None, dataFilter:str=None, cache:bool=False):
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
        df = fullDataset(tableID, limit, dataFilter)
    else:
        df = specificTable(tableID, name, limit, dataFilter)

    # if cache is true, check if cache folder exists, otherwise create it
    if cache:
        if not os.path.exists("./cache"):
            os.makedirs("./cache")
        df.to_csv(f"./cache/{tablename}.csv", index=False)
    return df

def fullDataset(tableID:str, limit:int=None, dataFilter:str=None):
    '''
    Get the full dataset of a table, including all codes
    tableID: str, the table ID of the dataset
    limit: int, the maximum number of rows to retrieve
    dataFilter: str, the filter to apply to the data
    '''
    rowAmount = limit if limit is not None else tableLengthObservations(tableID)
    df = getData(targetUrl(tableID, "Observations", limit, dataFilter), rowAmount)
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
    d = ['Observations']+[f"{df['name']}" for df in requests.get(tableUrl).json()['value'] if df['name'].endswith("Codes")]
    # if name not in d:
    #     raise ValueError(f"Invalid name '{name}'. Choose from {', '.join(d)}")
    target_url = f"{tableUrl}/{name}"
    if limit != None:
        target_url += f"?$top={limit}"
        target_url += f"&" if dataFilter != None else ""
    if dataFilter != None:
        # TODO check in the string dataFilter if the columnname is present and then add it to the filter, otherwise raise an error that the column name
        # is not in the df
        # example of dataFilter: "DimensionGroupId eq '123456789'"

        variableToFilter = dataFilter.split("eq")[0].strip()
        if variableToFilter not in d:
            raise ValueError(f"Invalid dataFilter '{dataFilter}'. Choose from {', '.join(d)}")
        

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