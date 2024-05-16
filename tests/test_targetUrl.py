import sys
sys.path.insert(0, '../src/cbsodatav4_kcvanderlinden/')
from src.cbsodatav4_kcvanderlinden import cbsodatav4

import pandas as pd

def testStandardUrl():
    url = cbsodatav4.targetUrl("80799ned", "Observations")
    assert url == "https://odata4.cbs.nl/CBS/80799ned/Observations"

def testSpecificTableUrl():
    url = cbsodatav4.targetUrl("80799ned", "PeriodenCodes")
    assert url == "https://odata4.cbs.nl/CBS/80799ned/PeriodenCodes"

def testSpecificTableUrlWithLimit():
    url = cbsodatav4.targetUrl("80799ned", "Observations", limit=10)
    assert url == "https://odata4.cbs.nl/CBS/80799ned/Observations?$top=10"

def testSpecificTableUrlWithFilter():
    url = cbsodatav4.targetUrl("80799ned", "Observations", dataFilter="Perioden eq '2018JJ00'")
    assert url == "https://odata4.cbs.nl/CBS/80799ned/Observations?$filter=Perioden eq '2018JJ00'"

def testSpecificTableUrlWithLimitAndFilter():
    url = cbsodatav4.targetUrl("80799ned", "Observations", limit=10, dataFilter="Perioden eq '2018JJ00'")
    assert url == "https://odata4.cbs.nl/CBS/80799ned/Observations?$top=10&?$filter=Perioden eq '2018JJ00'"

def testSpecificTableUrlWithLimitAndFilterValid():
    url = cbsodatav4.targetUrl("80799ned", "Observations", limit=10, dataFilter="Perioden eq '2018JJ00'")
    df = cbsodatav4.getData(url)

    assert list(df.columns) == [
        'Id', 
        "Measure",
        "ValueAttribute",
        "Value",
        "StringValue",
        "Persoonskenmerken",
        "Perioden"
        ]