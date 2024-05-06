import pandas as pd
import requests
import os

def get_odata(tableID:str, type:str="data", limit:int=None, filter:str=None):
    tableUrl = f"https://odata4.cbs.nl/CBS/{tableID}"
    d = {
        "data": "/Observations",
        # "Metadata": "/$metadata",
        # "groups": "/MeasureGroups"
    } | {df['name'].replace('Codes', ''):f"/{df['name']}" for df in requests.get(f"https://odata4.cbs.nl/CBS/{tableID}").json()['value'] if df['name'].endswith("Codes")}
    if type not in d:
        raise ValueError(f"Invalid type '{type}'. Choose from {', '.join(d.keys())}")
    target_url = tableUrl + d[type]
    if limit != None:
        target_url += f"?$top={limit}"
    if filter != None and type == "data":
        # TODO check for filter if column is in the data
        target_url += f"?$filter={filter}"
        
        
    data = pd.DataFrame()
    while target_url:
        r = requests.get(target_url).json()
        # data = data.append(pd.DataFrame(r['value'])) append is deprecated in next release
        data = pd.concat([data, pd.DataFrame(r['value'])], ignore_index=True)
        
        if '@odata.nextLink' in r:
            target_url = r['@odata.nextLink']
        else:
            target_url = None
            
    return data

def cbsodatav2(tableID, type="data", limit=None, filter=None, cache=True): # TODO change type parameter to only the types that are direct available, no translation
    if filter!=None:
        filter_as_string = filter.split("eq")[1].replace("'", "").strip()
        tablename = tableID + "_" + filter_as_string
    else:
        tablename = tableID
    # check for cached version if cache is True
    if cache:
        try:
            df = pd.read_csv(f"./cache/{tablename}_{type}.csv")
            return df
        except:
            pass
    # get data
    if type == "data":
        df = get_odata(tableID, type, limit, filter=filter)
        columns = {df['name'].replace('Codes', ''):f"/{df['name']}" for df in requests.get(f"https://odata4.cbs.nl/CBS/{tableID}").json()['value'] if df['name'].endswith("Codes")}
        for column in columns:
            codes = get_odata(tableID, column)
            df = pd.merge(df, codes, left_on=column, right_on="Identifier")
            if "PresentationType" in codes.columns:
                df.loc[df["PresentationType"]=="Relative", "Value"] = df["Value"] / 100 # TODO not always true, so disable
                df["Type waarde"] = df["PresentationType"]
            columnsToKeep = ["Title"]
            if column.startswith("Wijken"):
                df = df.rename(columns={column: "RegioCode"})
                df["GemeenteCode"] = df["DimensionGroupId"]
            if column == "RegioS":
                df = df.rename(columns={"Identifier": "RegioCode"})
            columnsToDrop = [col for col in list(set(codes.columns) - set(columnsToKeep))+ [column] if col in df.columns] 
            df = df.drop(columns=columnsToDrop)   
            df = df.rename(columns={"Title": column})
    # TODO an else statement for other types

    # check if cache folder exists, otherwisje create it
    if not os.path.exists("./cache"):
        os.makedirs("./cache")
    
    df.to_csv(f"./cache/{tablename}_{type}.csv", index=False)
    return df