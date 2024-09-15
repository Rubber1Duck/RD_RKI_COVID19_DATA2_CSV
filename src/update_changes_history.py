import os
import datetime as dt
import time as time
import pandas as pd
import numpy as np
import utils as ut
import gc
from multiprocess_pandas import applyparallel


def update(meta, BL, LK, mode="auto"):
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ": prepare history data. ",end="")
    t1 = time.time()
    HC_dtp = {"i": "str", "m": "int64", "c": "int64"}
    HD_dtp = {"i": "str", "m": "int64", "d": "int64"}
    HR_dtp = {"i": "str", "m": "int64", "r": "int64"}
    HI_dtp = {"i": "str", "m": "int64", "c7": "int64", "i7": "float"}

    HCC_dtp = {"i": "str", "m": "int64", "c": "int64", "dc": "int64", "cD": "int64"}
    HCD_dtp = {"i": "str", "m": "int64", "d": "int64", "cD": "int64"}
    HCR_dtp = {"i": "str", "m": "int64", "r": "int64", "cD": "int64"}
    HCI_dtp = {"i": "str", "m": "int64", "c7": "int64", "i7": "float", "cD": "int64"}
    

    timeStamp = meta["modified"]
    Datenstand = dt.datetime.fromtimestamp(timeStamp / 1000)
    Datenstand = Datenstand.replace(hour=0, minute=0, second=0, microsecond=0)

    base_path = os.path.dirname(os.path.abspath(__file__))
    if mode !="init":    
        BL = ut.read_file(meta["BL_url"])
        LK = ut.read_file(meta["LK_url"])
    
    # for smaler files rename fields
    # i = Id(Landkreis or Bundesland)
    # t = Name of Id(Landkreis or Bundesland)
    # m = Meldedatum
    # c = cases
    # d = deaths
    # r = recovered
    # c7 = cases7d (cases7days)
    # i7 = incidence7d (incidence7days)

    LK.rename(columns={"IdLandkreis": "i", "Meldedatum": "m", "cases": "c", "deaths": "d", "recovered": "r", "cases7d": "c7", "incidence7d": "i7"}, inplace=True)
    BL.rename(columns={"IdBundesland": "i", "Meldedatum": "m","cases": "c", "deaths": "d", "recovered": "r", "cases7d": "c7", "incidence7d": "i7"}, inplace=True)
    #convert dates to following numbers since 2020-01-01 to hold the files as short as possible
    LK["m"] = (pd.to_datetime(LK['m'], format='%Y-%m-%d') - pd.to_datetime('2020-01-01')).dt.days
    BL["m"] = (pd.to_datetime(BL['m'], format='%Y-%m-%d') - pd.to_datetime('2020-01-01')).dt.days

    # split LK
    LKcases = LK.copy()
    LKcases.drop(["d", "r", "c7", "i7"], inplace=True, axis=1)
    LKcases["c"] = LKcases["c"].astype("int64")
    LKdeaths = LK.copy()
    LKdeaths.drop(["c", "r", "c7", "i7"], inplace=True, axis=1)
    LKdeaths["d"] = LKdeaths["d"].astype("int64")
    LKrecovered = LK.copy()
    LKrecovered.drop(["c", "d", "c7", "i7"], inplace=True, axis=1)
    LKrecovered["r"] = LKrecovered["r"].astype("int64")
    LKincidence = LK.copy()
    LKincidence.drop(["c", "d", "r"], inplace=True, axis=1)
    LKincidence["c7"] = LKincidence["c7"].astype("int64")
    # free memory
    LK = pd.DataFrame()
    del LK
    gc.collect()

    # split BL
    BLcases = BL.copy()
    BLcases.drop(["d", "r", "c7", "i7"], inplace=True, axis=1)
    BLcases["c"] = BLcases["c"].astype("int64")
    BLdeaths = BL.copy()
    BLdeaths.drop(["c", "r", "c7", "i7"], inplace=True, axis=1)
    BLdeaths["d"] = BLdeaths["d"].astype("int64")
    BLrecovered = BL.copy()
    BLrecovered.drop(["c", "d", "c7", "i7"], inplace=True, axis=1)
    BLrecovered["r"] = BLrecovered["r"].astype("int64")
    BLincidence = BL.copy()
    BLincidence.drop(["c", "d", "r"], inplace=True, axis=1)
    BLincidence["c7"] = BLincidence["c7"].astype("int64")
    # free memory
    BL = pd.DataFrame()
    del BL
    gc.collect()
    t2 = time.time()
    print(f"Done in {round((t2 - t1), 3)} sec.")
    
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : Storeing history data. ",end="")
    t1=time.time()
    # store all files not compressed! will be done later
    historyPath = os.path.normpath(os.path.join(base_path, "..", "dataStore", "history"))
    
    LKFile = "districts.csv"
    BLFile = "states.csv"
    
    CasesPath = os.path.join(historyPath, "cases")
    DeathsPath = os.path.join(historyPath, "deaths")
    RecoveredPath = os.path.join(historyPath, "recovered")
    IncidencePath = os.path.join(historyPath, "incidence")
    
    LKcasesFull = os.path.join(CasesPath, LKFile)
    LKdeathsFull = os.path.join(DeathsPath, LKFile)
    LKrecoveredFull = os.path.join(RecoveredPath, LKFile)
    LKincidenceFull = os.path.join(IncidencePath, LKFile)
    
    BLcasesFull = os.path.join(CasesPath, BLFile)
    BLdeathsFull = os.path.join(DeathsPath, BLFile)
    BLrecoveredFull = os.path.join(RecoveredPath, BLFile)
    BLincidenceFull = os.path.join(IncidencePath, BLFile)

    # read oldLK(cases, deaths, recovered, incidence) if old file exist
    # write new data 
    if os.path.exists(LKcasesFull):
        oldLKcases = ut.read_csv(filename=LKFile, path=CasesPath, dtype=HC_dtp)
        oldLKcases["i"] = oldLKcases['i'].map('{:0>5}'.format)
        oldLKcases["m"] = oldLKcases['m'].astype("int64")
        oldLKcases["c"] = oldLKcases["c"].astype("int64")
    ut.write_csv(df=LKcases, filename=LKFile, path=CasesPath, dtype=HC_dtp)
    
    if os.path.exists(LKdeathsFull):
        oldLKdeaths = ut.read_csv(filename=LKFile, path=DeathsPath, dtype=HD_dtp)
        oldLKdeaths["i"] = oldLKdeaths['i'].map('{:0>5}'.format)
        oldLKdeaths['m'] = oldLKdeaths['m'].astype("int64")
        oldLKdeaths["d"] = oldLKdeaths["d"].astype("int64")
    ut.write_csv(df=LKdeaths, filename=LKFile, path=DeathsPath, dtype=HD_dtp)
        
    if os.path.exists(LKrecoveredFull):
        oldLKrecovered = ut.read_csv(filename=LKFile, path=RecoveredPath, dtype=HR_dtp)
        oldLKrecovered["i"] = oldLKrecovered['i'].map('{:0>5}'.format)
        oldLKrecovered['m'] = oldLKrecovered['m'].astype("int64")
        oldLKrecovered["r"] = oldLKrecovered["r"].astype("int64")
    ut.write_csv(df=LKrecovered, filename=LKFile, path=RecoveredPath, dtype=HR_dtp)
        
    if os.path.exists(LKincidenceFull):
        oldLKincidence = ut.read_csv(filename=LKFile, path=IncidencePath, dtype=HI_dtp)
        oldLKincidence["i"] = oldLKincidence['i'].map('{:0>5}'.format)
        oldLKincidence['m'] = oldLKincidence['m'].astype("int64")
        oldLKincidence["c7"] = oldLKincidence["c7"].astype("int64")
    ut.write_csv(df=LKincidence, filename=LKFile, path=IncidencePath, dtype=HI_dtp)
        
    # read oldBL(cases, deaths, recovered, incidence) if old file exist
    # write new data
    if os.path.exists(BLcasesFull):
        oldBLcases = ut.read_csv(filename=BLFile, path=CasesPath, dtype=HC_dtp)
        oldBLcases["i"] = oldBLcases['i'].map('{:0>2}'.format)
        oldBLcases['m'] = oldBLcases['m'].astype("int64")
        oldBLcases["c"] = oldBLcases["c"].astype("int64")
    ut.write_csv(df=BLcases, filename= BLFile, path=CasesPath, dtype=HC_dtp)
       
    if os.path.exists(BLdeathsFull):
        oldBLdeaths = ut.read_csv(filename=BLFile, path=DeathsPath, dtype=HD_dtp)
        oldBLdeaths["i"] = oldBLdeaths['i'].map('{:0>2}'.format)
        oldBLdeaths["m"] = oldBLdeaths["m"].astype("int64")
        oldBLdeaths["d"] = oldBLdeaths["d"].astype("int64")
    ut.write_csv(df=BLdeaths, filename=BLFile, path=DeathsPath, dtype=HD_dtp)
       
    if os.path.exists(BLrecoveredFull):
        oldBLrecovered = ut.read_csv(filename=BLFile, path=RecoveredPath, dtype=HR_dtp)
        oldBLrecovered["i"] = oldBLrecovered['i'].map('{:0>2}'.format)
        oldBLrecovered["m"] = oldBLrecovered["m"].astype("int64")
        oldBLrecovered["r"] = oldBLrecovered["r"].astype("int64")
    ut.write_csv(df=BLrecovered, filename=BLFile, path=RecoveredPath, dtype=HR_dtp)
    
    if os.path.exists(BLincidenceFull):
        oldBLincidence = ut.read_csv(filename=BLFile, path=IncidencePath, dtype=HI_dtp)
        oldBLincidence["i"] = oldBLincidence['i'].map('{:0>2}'.format)
        oldBLincidence["m"] = oldBLincidence["m"].astype("int64")
        oldBLincidence["c7"] = oldBLincidence["c7"].astype("int64")
    ut.write_csv(df=BLincidence, filename=BLFile, path=IncidencePath, dtype=HI_dtp)
    t2 = time.time()
    print(f"Done in {round((t2 - t1), 3)} sec.")
    
    # calculate diff data
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : calculating history difference. ",end="")
    t1 = time.time()
    changesPath = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges"))
    try:
        LKDiffCases = ut.get_different_rows(oldLKcases, LKcases)
        LKDiffCases.set_index(["i", "m"], inplace=True, drop=False)
        oldLKcases.set_index(["i", "m"], inplace=True, drop=False)
        LKDiffCases["dc"] = LKDiffCases["c"] - oldLKcases["c"]
        LKDiffCases["dc"] = LKDiffCases["dc"].fillna(LKDiffCases["c"])
    except:
        LKDiffCases = LKcases.copy()
        LKDiffCases["dc"] = LKDiffCases["c"]
    
    try:
        LKDiffDeaths = ut.get_different_rows(oldLKdeaths, LKdeaths)
    except:
        LKDiffDeaths = LKdeaths.copy()
    
    try:
        LKDiffRecovered = ut.get_different_rows(oldLKrecovered, LKrecovered)
    except:
        LKDiffRecovered = LKrecovered.copy()
    
    try:
        # dont compare float values
        oldLKincidence.drop("i7", inplace=True, axis=1)
        temp = LKincidence.copy()
        LKincidence.drop("i7", inplace=True, axis=1)
        LKDiffIncidence = ut.get_different_rows(oldLKincidence, LKincidence)
        LKDiffIncidence.set_index(["i","m"], inplace=True, drop=False)
        temp.set_index(["i","m"], inplace=True, drop=True)
        LKDiffIncidence["i7"] = temp["i7"]
        LKDiffIncidence.reset_index(inplace=True, drop=True)
    except:
        LKDiffIncidence = LKincidence.copy()

    try:
        BLDiffCases = ut.get_different_rows(oldBLcases, BLcases)
        BLDiffCases.set_index(["i", "m"], inplace=True, drop=False)
        oldBLcases.set_index(["i","m"], inplace=True, drop=False)
        BLDiffCases["dc"] = BLDiffCases["c"] - oldBLcases["c"]
        BLDiffCases["dc"] = BLDiffCases["dc"].fillna(BLDiffCases["c"])
    except:
        BLDiffCases = BLcases.copy()
        BLDiffCases["dc"] = BLDiffCases["c"]
    
    try:
        BLDiffDeaths = ut.get_different_rows(oldBLdeaths, BLdeaths)
    except:
        BLDiffDeaths = BLdeaths.copy()
    
    try:
        BLDiffRecovered = ut.get_different_rows(oldBLrecovered, BLrecovered)
    except:
        BLDiffRecovered = BLrecovered.copy()
    
    try:
        oldBLincidence.drop("i7", inplace=True, axis=1)
        temp = BLincidence.copy()
        BLincidence.drop("i7", inplace=True, axis=1)
        BLDiffIncidence = ut.get_different_rows(oldBLincidence, BLincidence)
        BLDiffIncidence.set_index(["i","m"], inplace=True, drop=False)
        temp.set_index(["i","m"], inplace=True, drop=True)
        BLDiffIncidence["i7"] = temp["i7"]
        BLDiffIncidence.reset_index(inplace=True, drop=True)
    except:
        BLDiffIncidence = BLincidence.copy()

    t2 = time.time()
    print(f"Done in {round((t2 - t1), 3)} sec.")
    
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : storing change history. ",end="")
    t1 = time.time()
    ChangeDateInt = (Datenstand - pd.to_datetime('2020-01-01')).days
    LKDiffCases["cD"] = ChangeDateInt
    LKDiffDeaths["cD"] = ChangeDateInt
    LKDiffRecovered["cD"] = ChangeDateInt
    LKDiffIncidence["cD"] = ChangeDateInt
    
    BLDiffCases["cD"] = ChangeDateInt
    BLDiffDeaths["cD"] = ChangeDateInt
    BLDiffRecovered["cD"] = ChangeDateInt
    BLDiffIncidence["cD"] = ChangeDateInt
    
    LKDiffFile = "districts_Diff.csv"
    BLDiffFile = "states_Diff.csv"

    DiffCasesPath = os.path.join(changesPath, "cases")
    DiffDeathsPath = os.path.join(changesPath, "deaths")
    DiffRecoveredPath = os.path.join(changesPath, "recovered")
    DiffIncidencePath = os.path.join(changesPath, "incidence")
    
    LKDiffCasesFull = os.path.join(DiffCasesPath, LKDiffFile)
    LKDiffDeathsFull = os.path.join(DiffDeathsPath, LKDiffFile)
    LKDiffRecoveredFull = os.path.join(DiffRecoveredPath, LKDiffFile)
    LKDiffIncidenceFull = os.path.join(DiffIncidencePath, LKDiffFile)

    BLDiffCasesFull = os.path.join(DiffCasesPath, BLDiffFile)
    BLDiffDeathsFull = os.path.join(DiffDeathsPath, BLDiffFile)
    BLDiffRecoveredFull = os.path.join(DiffRecoveredPath, BLDiffFile)
    BLDiffIncidenceFull = os.path.join(DiffIncidencePath, BLDiffFile)
    
    LKDiffCases["dc"] = LKDiffCases["dc"].astype(int)
    LKDiffCases.reset_index(inplace=True, drop=True)
    LKDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
    if os.path.exists(LKDiffCasesFull):
        ut.write_csv(df=LKDiffCases, filename=LKDiffFile, path=DiffCasesPath, dtype=HCC_dtp, mode='a')
    else:
        ut.write_csv(df=LKDiffCases, filename=LKDiffFile, path=DiffCasesPath, dtype=HCC_dtp)

    LKDiffDeaths.reset_index(inplace=True, drop=True)
    LKDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
    if os.path.exists(LKDiffDeathsFull):
        ut.write_csv(df=LKDiffDeaths, path=DiffDeathsPath, filename=LKDiffFile, dtype=HCD_dtp, mode='a')
    else:
        ut.write_csv(df=LKDiffDeaths, path=DiffDeathsPath, filename=LKDiffFile, dtype=HCD_dtp)

    LKDiffRecovered.reset_index(inplace=True, drop=True)
    LKDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
    if os.path.exists(LKDiffRecoveredFull):
        ut.write_csv(df=LKDiffRecovered, path=DiffRecoveredPath, filename=LKDiffFile, dtype=HCR_dtp, mode='a')
    else:
        ut.write_csv(df=LKDiffRecovered, path=DiffRecoveredPath, filename=LKDiffFile, dtype=HCR_dtp)

    LKDiffIncidence.reset_index(inplace=True, drop=True)
    LKDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
    if os.path.exists(LKDiffIncidenceFull):
        ut.write_csv(df=LKDiffIncidence, path=DiffIncidencePath, filename=LKDiffFile, dtype=HCI_dtp, mode='a')
    else:
        ut.write_csv(df=LKDiffIncidence, path=DiffIncidencePath, filename=LKDiffFile, dtype=HCI_dtp)
    
    BLDiffCases["dc"] = BLDiffCases["dc"].astype(int)
    BLDiffCases.reset_index(inplace=True, drop=True)
    BLDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
    if os.path.exists(BLDiffCasesFull):
        ut.write_csv(df=BLDiffCases, path=DiffCasesPath, filename=BLDiffFile, dtype=HCC_dtp, mode='a')
    else:
        ut.write_csv(df=BLDiffCases, path=DiffCasesPath, filename=BLDiffFile, dtype=HCC_dtp)

    BLDiffDeaths.reset_index(inplace=True, drop=True)
    BLDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
    if os.path.exists(BLDiffDeathsFull):
        ut.write_csv(df=BLDiffDeaths, path=DiffDeathsPath, filename=BLDiffFile, dtype=HCD_dtp, mode='a')
    else:
        ut.write_csv(df=BLDiffDeaths, path=DiffDeathsPath, filename=BLDiffFile, dtype=HCD_dtp)

    BLDiffRecovered.reset_index(inplace=True, drop=True)
    BLDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
    if os.path.exists(BLDiffRecoveredFull):
        ut.write_csv(df=BLDiffRecovered, path=DiffRecoveredPath, filename=BLDiffFile, dtype=HCR_dtp, mode='a')
    else:
        ut.write_csv(df=BLDiffRecovered, path=DiffRecoveredPath, filename=BLDiffFile, dtype=HCR_dtp)
    
    BLDiffIncidence.reset_index(inplace=True, drop=True)
    BLDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
    if os.path.exists(BLDiffIncidenceFull):
        ut.write_csv(df=BLDiffIncidence, path=DiffIncidencePath, filename=BLDiffFile, dtype=HCI_dtp, mode='a')
    else:
        ut.write_csv(df=BLDiffIncidence, path=DiffIncidencePath, filename=BLDiffFile, dtype=HCI_dtp)

    t2 = time.time()
    print(f"Done in {round((t2 - t1), 3)} sec.")

    return

def update_mass(meta):
    base_path = os.path.dirname(os.path.abspath(__file__))
    
    BV_csv_path = os.path.join(base_path, "..", "Bevoelkerung", "Bevoelkerung.csv")
    BV_dtypes = {"AGS": "str", "Altersgruppe": "str", "Name": "str", "GueltigAb": "object", "GueltigBis": "object",
                 "Einwohner": "Int32", "männlich": "Int32", "weiblich": "Int32"}
    CV_dtypes = {"IdLandkreis": "str", "NeuerFall": "Int32", "NeuerTodesfall": "Int32", "NeuGenesen": "Int32",
                 "AnzahlFall": "Int32", "AnzahlTodesfall": "Int32", "AnzahlGenesen": "Int32", "Meldedatum": "object"}

    # open bevoelkerung.csv
    BV = pd.read_csv(BV_csv_path, usecols=BV_dtypes.keys(), dtype=BV_dtypes)
    BV["GueltigAb"] = pd.to_datetime(BV["GueltigAb"])
    BV["GueltigBis"] = pd.to_datetime(BV["GueltigBis"])

    BV_BL = BV[BV["AGS"].str.len() == 2].copy()
    BV_BL.reset_index(inplace=True, drop=True)

    BV_BL_A00 = BV_BL[BV_BL["Altersgruppe"] == "A00+"].copy()
    BV_BL_A00.reset_index(inplace=True, drop=True)

    BV_LK = BV[BV["AGS"].str.len() == 5].copy()
    BV_LK.reset_index(inplace=True, drop=True)

    BV_LK_A00 = BV_LK[BV_LK["Altersgruppe"] == "A00+"].copy()
    BV_LK_A00.reset_index(inplace=True, drop=True)

    # load covid latest from web
    t1 = time.time()
    fileName = meta["filename"]
    fileSize = int(meta["filesize"])
    fileNameFull = meta["filepath"]
    timeStamp = meta["modified"]
    Datenstand = dt.datetime.fromtimestamp(timeStamp / 1000)
    Datenstand = Datenstand.replace(hour=0, minute=0, second=0, microsecond=0)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : loading {fileName} (size: {fileSize} bytes) to dataframe. ", end="")
    
    LK = pd.read_csv(fileNameFull, engine="pyarrow", usecols=CV_dtypes.keys(), dtype=CV_dtypes)
    LK.sort_values(by=["IdLandkreis", "Meldedatum"], axis=0, inplace=True, ignore_index=True)
    LK.reset_index(drop=True, inplace=True)

    # ----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
    LK = ut.squeeze_dataframe(LK)

    
    t2 = time.time()
    print(f"Done in {round((t2 - t1), 3)} secs. {LK.shape[0]} rows. {round(LK.shape[0] / (t2 - t1), 1)} rows/sec.")
    
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : add missing columns. ", end="")
    t1 = time.time()
    LK["IdLandkreis"] = LK['IdLandkreis'].map('{:0>5}'.format)
    LK.insert(loc=0, column="IdBundesland", value=LK["IdLandkreis"].str.slice(0,2))
    #LK["Meldedatum"] = pd.to_datetime(LK["Meldedatum"]).dt.date
    
    # ----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
    LK = ut.squeeze_dataframe(LK)
    
    t2 = time.time()
    print(f"Done in {round((t2 - t1), 3)} secs.")

    # History
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : calculating history data. ", end="")
    t1 = time.time()
    
    # used keylists
    key_list_LK_hist = ["IdLandkreis", "Meldedatum"]
    key_list_BL_hist = ["IdBundesland", "Meldedatum"]
    key_list_ID0_hist = ["Meldedatum"]

    LK["AnzahlFall"] = np.where(LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0).astype(int)
    LK["AnzahlTodesfall"] = np.where(LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0).astype(int)
    LK["AnzahlGenesen"] = np.where(LK["NeuGenesen"].isin([1, 0, -9]), LK["AnzahlGenesen"], 0).astype(int)
    LK.drop(["NeuerFall", "NeuerTodesfall", "NeuGenesen"], inplace=True, axis=1)
    LK.rename(columns={"AnzahlFall": "cases", "AnzahlTodesfall": "deaths", "AnzahlGenesen": "recovered"}, inplace=True)
    agg_key = {
        c: "max" if c in ["IdBundesland"] else "sum"
        for c in LK.columns
        if c not in key_list_LK_hist
    }
    LK = LK.groupby(by=key_list_LK_hist, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: "max" if c in ["IdLandkreis"] else "sum"
        for c in LK.columns
        if c not in key_list_BL_hist
    }
    BL = LK.groupby(by=key_list_BL_hist, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: "max" if c in ["IdBundesland", "IdLandkreis"] else "sum"
        for c in BL.columns
        if c not in key_list_ID0_hist
    }
    ID0 = BL.groupby(by=key_list_ID0_hist, as_index=False, observed=True).agg(agg_key)
    LK.drop(["IdBundesland"], inplace=True, axis=1)
    LK.sort_values(by=key_list_LK_hist, inplace=True)
    LK.reset_index(inplace=True, drop=True)
    BL.drop(["IdLandkreis"], inplace=True, axis=1)
    ID0.drop(["IdLandkreis"], inplace=True, axis=1)
    ID0["IdBundesland"] = "00"
    BL = pd.concat([ID0, BL])
    BL.sort_values(by=key_list_BL_hist, inplace=True)
    BL.reset_index(inplace=True, drop=True)
    
    # fill dates for every region
    startDate = "2020-01-01"
    date_range_str = []
    for datum in pd.date_range(end=(Datenstand - dt.timedelta(days=1)), start=startDate).to_list():
        date_range_str.append(datum.strftime("%Y-%m-%d"))
    allDates = pd.DataFrame(date_range_str, columns=["Meldedatum"])
    BLDates = pd.DataFrame(pd.unique(BL["IdBundesland"]).copy(), columns=["IdBundesland"])
    LKDates = pd.DataFrame(pd.unique(LK["IdLandkreis"]).copy(), columns=["IdLandkreis"])
    # add Einwohner
    BV_mask = ((BV_BL_A00["AGS"].isin(BLDates["IdBundesland"])) & (BV_BL_A00["GueltigAb"] <= Datenstand) & (BV_BL_A00["GueltigBis"] >= Datenstand))
    BV_masked = BV_BL_A00[BV_mask].copy()
    BV_masked.drop(["GueltigAb", "GueltigBis", "Altersgruppe", "männlich", "weiblich", "Name"], inplace=True, axis=1)
    BV_masked.rename(columns={"AGS": "IdBundesland"}, inplace=True)
    BLDates = BLDates.merge(right=BV_masked, on=["IdBundesland"], how="left")

    BV_mask = ((BV_LK_A00["AGS"].isin(LK["IdLandkreis"])) & (BV_LK_A00["GueltigAb"] <= Datenstand) & (BV_LK_A00["GueltigBis"] >= Datenstand))
    BV_masked = BV_LK_A00[BV_mask].copy()
    BV_masked.drop(["GueltigAb", "GueltigBis", "Altersgruppe", "männlich", "weiblich"], inplace=True, axis=1)
    BV_masked.rename(columns={"AGS": "IdLandkreis", "Name": "Landkreis"}, inplace=True)
    LKDates = LKDates.merge(right=BV_masked, on="IdLandkreis", how="left")

    BLDates = BLDates.merge(allDates, how="cross")
    BLDates = ut.squeeze_dataframe(BLDates)
    LKDates = LKDates.merge(allDates, how="cross")
    LKDates = ut.squeeze_dataframe(LKDates)
    
    BL = BL.merge(BLDates, how="right", on=["IdBundesland", "Meldedatum"])
    LK = LK.merge(LKDates, how="right", on=["IdLandkreis", "Meldedatum"])
    LK = LK[LK["Landkreis"].notna()]
    LK.drop(["Landkreis"], inplace=True, axis=1)

    # clear unneeded data
    ID0 = pd.DataFrame()
    allDates = pd.DataFrame()
    BLDates = pd.DataFrame()
    LKDates = pd.DataFrame()
    BV_mask = pd.DataFrame()
    BV_masked = pd.DataFrame()
    del ID0
    del allDates
    del BLDates
    del LKDates
    del BV_mask
    del BV_masked
    gc.collect()

    #fill nan with 0
    BL["cases"] = BL["cases"].fillna(0).astype(int)
    BL["deaths"] = BL["deaths"].fillna(0).astype(int)
    BL["recovered"] = BL["recovered"].fillna(0).astype(int)

    LK["cases"] = LK["cases"].fillna(0).astype(int)
    LK["deaths"] = LK["deaths"].fillna(0).astype(int)
    LK["recovered"] = LK["recovered"].fillna(0).astype(int)

    
    t2 = time.time()
    print(f"Done in {round((t2 - t1), 3)} secs.")
    
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : calculating BL incidence. {BL.shape[0]} rows. ",end="")
    t1 = time.time()
    BL["Meldedatum"] = BL["Meldedatum"].astype(str)
    BL = BL.groupby(["IdBundesland"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    BL.reset_index(inplace=True, drop=True)
    BL.sort_values(["IdBundesland", "Meldedatum"], inplace=True, axis=0)
    BL.reset_index(inplace=True, drop=True)
    BL["i7"] = (BL["c7"] / BL["Einwohner"] * 100000).round(5)
    BL.drop(["Einwohner"], inplace=True, axis=1)
    t2 = time.time()
    print(f"Done in {round(t2 - t1, 3)} sec.")
    
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : calculating LK incidence. {LK.shape[0]} rows. ",end="")
    t1 = time.time()
    LK["Meldedatum"] = LK["Meldedatum"].astype(str)
    LK = LK.groupby(["IdLandkreis"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    LK.reset_index(inplace=True, drop=True)
    LK.sort_values(["IdLandkreis", "Meldedatum"], inplace=True, axis=0)
    LK.reset_index(inplace=True, drop=True)
    LK["i7"] = (LK["c7"] / LK["Einwohner"] * 100000).round(5)
    LK.drop(["Einwohner"], inplace=True, axis=1)
    t2 = time.time()
    print(f"Done in {round(t2 - t1, 3)} sec.")
    
    update(meta=meta, BL=BL, LK=LK, mode="init")
    
    return
