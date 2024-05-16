import sys
sys.path.insert(0, '../src/cbsodatav4_kcvanderlinden/')
from src.cbsodatav4_kcvanderlinden import cbsodatav4

import pandas as pd

def test_outcomeIsDF():
    df = cbsodatav4.specificTable("80799ned", "PeriodenCodes")

    assert isinstance(df, pd.DataFrame)

def test_columnsAreCorrect():
    df = cbsodatav4.specificTable("80799ned", "PeriodenCodes")

    assert list(df.columns) == [
        'Identifier', 
        'Index',
        'Title',
        'Description',
        'DimensionGroupId',
        'Status'
        ]
    
def test_lengthIsCorrect():
    df = cbsodatav4.specificTable("80799ned", "PeriodenCodes")

    assert len(df) == 13