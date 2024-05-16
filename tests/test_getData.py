# test the getData module
#
# test_getData.py
import sys
sys.path.insert(0, '../src/cbsodatav4_kcvanderlinden/')
from src.cbsodatav4_kcvanderlinden import cbsodatav4

import pandas as pd

def testOutcomeIsDF():
    df = cbsodatav4.getData("https://odata4.cbs.nl/CBS/80799ned/Observations")

    assert isinstance(df, pd.DataFrame)

def testLengthIsCorrect():
    df = cbsodatav4.getData("https://odata4.cbs.nl/CBS/80799ned/Observations")

    assert len(df) == 288

def testColumnsAreCorrect():
    df = cbsodatav4.getData("https://odata4.cbs.nl/CBS/80799ned/Observations")

    assert list(df.columns) == [
        'Id', 
        "Measure",
        "ValueAttribute",
        "Value",
        "StringValue",
        "Persoonskenmerken",
        "Perioden"
        ]