import argparse
import json
import requests
import csv
import pandas as pd
from functools import lru_cache


# geonames username
global_username = "scriptie_vdwal"


@lru_cache(maxsize=None)
def findID_baseline(placeName):
    ''' returns GeoNames ID for the baseline program '''

    URL = ("http://api.geonames.org/searchJSON?q="
          + placeName
          + "&maxRows=1&username="
          + global_username)
    response = requests.get(URL)
    data = response.json()

    if len(data) == 0:    # no data is returned
        return 0

    geodata = data["geonames"]
    if len(geodata) == 0:    # no results are found
        return 0

    result = geodata[0]
    id = result["geonameId"]

    return id


def makeString(codesList):
    ''' returns a string that can be inserted in a GeoNames query '''

    codesStr = ''
    codesSet = set(codesList)

    # list of countries is empty, so we default to NL
    if len(codesSet) == 0:
        codesStr += "&countryBias=NL"
        return codesStr

    # list of countries contains 1 item
    if len(codesSet) == 1:
        codesStr += "&countryBias="
        codesStr += codesList[0]
        return codesStr

    # list of countries contains multiple items
    for code in codesSet:
        codesStr += "&country="
        codesStr += code
        return codesStr


@lru_cache(maxsize=None)    # save result in cache memory
def findID(placeName, codes=False):
    '''
    return GeoNames ID that matches with given placeName
    if no country codes are given, return country code of matched location
    else use country codes to refine query
    '''

    urlBase = "http://api.geonames.org/searchJSON?q="

    if codes:   # find IDs for non-countries
        URL = (urlBase
              + placeName
              + "&searchlang=nl"
              + codes
              + "&maxRows=1&username="
              + global_username)

        response = requests.get(URL)
        print(response.status_code)
        data = response.json()

        if len(data) == 0:
            return 0

        geodata = data["geonames"]
        if len(geodata) == 0:
            # if no geodata was found, retry with a simpler query
            id = findID_baseline(placeName)
            return id

        firstOption = geodata[0]
        id = 0
        id = firstOption["geonameId"]
        return id

    # find country IDs
    URL = (urlBase
          + placeName
          + "&searchlang=nl&maxRows=2&username="
          + global_username)
    response = requests.get(URL)
    print(response.status_code)

    data = response.json()
    if len(data) == 0:    # no data is returned
        return 0, []

    geodata = data["geonames"]
    if len(geodata) == 0:    # no results are found
        return 0, []

    firstOption = geodata[0]
    id = 0

    if firstOption["fcode"] == "CONT":    # result is of type `continent'
        id = firstOption["geonameId"]
        return id, []

    for option in geodata:
        if option["fcode"].startswith("PCL"):    # result is of type `political entity'
            id = option["geonameId"]
            if "countryCode" in option:
                countryCode = [option["countryCode"]]
            else:
                countryCode = []
            return id, countryCode

    return id, []


def processArticle(data):
    '''
    processes annotations that belong to the same article
    returns the input data with added GeoNames IDs
    '''

    print("\nprocessing article", data['articleID'].iloc[0], "...")

    processedData = data.copy()
    processedData["predID"] = 0
    codes = []    # list of countries that are present in the current article

    # find IDs for countries and add their country code to a list
    for index, row in data.iterrows():
        predID, countryCode = findID(row.toponym)
        processedData.at[index, "predID"] = predID
        codes += countryCode

    # process country codes so it can be inserted in a URL
    codesURL = makeString(codes)

    # find IDs for the rest of the data
    for index, row in processedData[processedData["predID"]==0].iterrows():
        predID = findID(row.toponym, codesURL)
        processedData.at[index, "predID"] = predID

    return processedData



def readTSV(dataset, baseline):
    '''
    processes the given tsv file
    returns a dataset with added GeoName IDs
    '''

    with open(dataset, newline='') as f:
        columnNames = ["articleID", "toponym", "geoID", "isTitle"]
        df = pd.read_csv(f, sep='\t', names=columnNames)

        if baseline:    # perform the baseline program
            print("performing the baseline program on", dataset, "...")
            df["predID"] = df["toponym"].apply(findID_baseline)

        else:    # perform the normal program
            print("performing the normal program on", dataset)
            df = df.groupby("articleID").apply(processArticle)

    return df


def writeTSV(dataframe):
    ''' writes pandas dataframe to output.tsv '''
    dataframe.to_csv("output.tsv", sep='\t', index=False)


def agreement(df):
    ''' calculates agreement between expected and predicted GeoNames IDs '''

    total = len(df)    # total number of annotations
    totalCorrect = 0    # total number of correctly guessed geoIDs
    notGuessed = 0    # total number of geoIDs that were not guessed
    for row in df.itertuples():
        expID = row.geoID
        predID = row.predID

        if expID == predID:
            totalCorrect += 1

        elif predID == 0:
            notGuessed += 1

    totalGuessed = total - notGuessed    # total number of guessed geoIDs

    precision = totalCorrect / totalGuessed
    recall = totalCorrect / total
    f1 = (2 * precision * recall) / (precision + recall)

    print("\nAgreement between the guessed geoIDs and expected geoIDs:\n"
          "\nTotal number of annotations: {}".format(total),
          "\nPrecision: {}%".format(precision),
          "\nRecall: {}".format(recall),
          "\nF1-score: {}%".format(f1))


def main():

    # commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, help="a dataset in tsv format", default="devset.tsv")
    parser.add_argument("--baseline", action="store_true", help="perform the baseline program", default=False)
    parser.add_argument("--username", type=str, help="geonames username", default="scriptie_vdwal")
    args = parser.parse_args()

    # set variables
    dataset = args.dataset
    global global_username
    global_username = args.username

    # process the data
    processed_dataset = readTSV(dataset, args.baseline)
    writeTSV(processed_dataset)
    agreement(processed_dataset)

    # clear cache when program is done running
    findID.cache_clear()
    findID_baseline.cache_clear()

if __name__ == "__main__":
    main()
