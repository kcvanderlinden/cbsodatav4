import sys
sys.path.insert(0, '../src/cbsodatav4_kcvanderlinden/')
from src.cbsodatav4_kcvanderlinden import cbsodatav4

import pandas as pd

def test_SimpleTableLengthObservations():
    """
    Test the tableLengthObservations function with a simple tableID.
    """
    tableID = '80799ned'
    
    result = cbsodatav4.tableLengthObservations(tableID)
    
    assert result == 288