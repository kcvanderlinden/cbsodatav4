import sys
sys.path.insert(0, '../src/cbsodatav4_kcvanderlinden/')
from src.cbsodatav4_kcvanderlinden import cbsodatav4

import pandas as pd

def test_SimpleTableName():
    """
    Test the tableName function with a simple tableID.
    """
    tableID = '37296ned'
    name = 'Data'
    dataFilter = None
    limit = None
    
    result = cbsodatav4.tableName(tableID, name, dataFilter, limit)
    
    assert result == '37296ned_Data'

def test_TableNameWithFilter():
    """
    Test the tableName function with a tableID and a dataFilter.
    """
    tableID = '37296ned'
    name = 'Data'
    dataFilter = "Perioden eq '2019JJ00'"
    limit = None
    
    result = cbsodatav4.tableName(tableID, name, dataFilter, limit)
    
    assert result == '37296ned_Data_Perioden_2019JJ00'

def test_TableNameWithLimit():
    """
    Test the tableName function with a tableID and a limit.
    """
    tableID = '37296ned'
    name = 'Data'
    dataFilter = None
    limit = 1000
    
    result = cbsodatav4.tableName(tableID, name, dataFilter, limit)
    
    assert result == '37296ned_Data_limit=1000'

def test_TableNameWithFilterAndLimit():
    """
    Test the tableName function with a tableID, a dataFilter, and a limit.
    """
    tableID = '37296ned'
    name = 'Data'
    dataFilter = "Perioden eq '2019JJ00'"
    limit = 1000
    
    result = cbsodatav4.tableName(tableID, name, dataFilter, limit)
    
    assert result == '37296ned_Data_Perioden_2019JJ00_limit=1000'