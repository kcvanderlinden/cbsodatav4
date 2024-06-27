import pandas as pd
import requests
import concurrent.futures
import os
import math

def cbsConnect(target_url:str):
    '''
    This function connects to the CBS API and returns a list of dictionaries.
    target_url: str, the URL is the url that contains the data you want to retrieve
    returns: list
    '''
    result = (requests.get(target_url).json())['value']
    return result

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

    # check if cache folder exists, otherwise create it
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
    code_columns = [f"{df['name']}" for df in requests.get(f"https://odata4.cbs.nl/CBS/{tableID}").json()['value'] if df['name'].endswith("Codes")]
    group_columns = [f"{df['name']}" for df in requests.get(f"https://odata4.cbs.nl/CBS/{tableID}").json()['value'] if df['name'].endswith("Groups")]
    columnsToDrop = []
    for column in code_columns+group_columns:
        codes = getData(targetUrl(tableID, column)) # ["Identifier", "Title", "MeasureGroupId"]
        columnCleanName = column.replace("Codes", "")
        df = pd.merge(df, codes, left_on=columnCleanName, right_on="Identifier")
        # if "PresentationType" in codes.columns:
            # df.loc[df["PresentationType"]=="Relative", "Value"] = df["Value"] / 100 # TODO "Unit" gebruiken om te bepalen hoe getal bewerkt moet worden.
            # df["Type waarde"] = df["PresentationType"]
        columnsToKeep = ["Title"]
        if column.startswith("Wijken"):
            df = df.rename(columns={column: "RegioCode"})
            df["GemeenteCode"] = df["DimensionGroupId"]
            df['RegioCode'] = df['WijkenEnBuurten']
        if column.lower().endswith("regioscodes"):
            df[column] = df[column.replace("Codes", "")].copy()
            columnsToKeep += column
        # if column == "RegioSCodes":
        #     df = df.rename(columns={"RegioS": "RegioCode"})
        columnsToDrop += [col for col in list(set(codes.columns) - set(columnsToKeep))+ [column.replace("Codes", "")] if col in df.columns]    
        df = df.rename(columns={"Title": columnCleanName})
    df = df.drop(columns=columnsToDrop)
    df = df.drop(columns=['ValueAttribute', 'StringValue']) # TODO check if these columns are always present
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
    observationCount = requests.get(f"https://odata4.cbs.nl/CBS/{tableID}/Properties").json()['ObservationCount']
    return observationCount

def targetUrl(tableID:str, name:str, limit:int=None, dataFilter:str=None):
    '''
    Create the target URL for the API request.
    tableID: str, the table ID of the dataset
    name: str, the name of the table to retrieve
    limit: int, the maximum number of rows to retrieve
    dataFilter: str, the filter to apply to the data
    '''
    tableUrl = f"https://odata4.cbs.nl/CBS/{tableID}"
    # d = ['Observations']+[f"{df['name']}" for df in requests.get(f"https://odata4.cbs.nl/CBS/{tableID}").json()['value'] if df['name'].endswith("Codes")]
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




