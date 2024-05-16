import sys
sys.path.insert(0, '../src/cbsodatav4_kcvanderlinden/')
from src.cbsodatav4_kcvanderlinden import cbsodatav4

import pandas as pd

def test_outcomeIsDataFrame():
    """
    Test if the outcome of the DataFrame function is a DataFrame.
    """
    tableID = '80799ned'
    
    result = cbsodatav4.fullDataset(tableID)
    
    assert isinstance(result, pd.DataFrame)

def test_columnsAreCorrect():
    """
    Test if the columns of the DataFrame are correct.
    """
    tableID = '85797NED'
    
    result = cbsodatav4.fullDataset(tableID, limit=10)
    
    assert list(result.columns) == [
        'Id', 'Value', 'Measure', 
        'Geslacht', 'Leeftijd', 'RegioS',
        'RegioSCodes', 'Perioden'
        ]
    
def test_DataFrameLength():
    """
    Test if the columns of the DataFrame are correct.
    """
    tableID = '85797NED'
    
    result = cbsodatav4.fullDataset(tableID, limit=10)

    assert len(result) == 10