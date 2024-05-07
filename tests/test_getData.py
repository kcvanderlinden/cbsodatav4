# test the getData module
#
# test_getData.py
import unittest
from unittest.mock import patch, Mock
from ..src.cbsodatav4_kcvanderlinden import cbsodatav4

import pandas as pd

def testOutcomeIsDF():
    tableID = "84929NED"
    limit = None
    dataFilter = None

    df = cbsodatav4.getData(tableID, limit, dataFilter)

    assert isinstance(df, pd.DataFrame)