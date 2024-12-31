import os
import datetime as dt
import time as time
import pandas as pd
import numpy as np
import utils as ut
from multiprocess_pandas import applyparallel


def update(meta, BL, LK, mode="auto"):
        
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
        LK.rename(columns={"IdLandkreis": "i", "Meldedatum": "m", "cases": "c", "deaths": "d", "recovered": "r", "cases7d": "c7", "incidence7d": "i7"}, inplace=True)
        BL.rename(columns={"IdBundesland": "i", "Meldedatum": "m","cases": "c", "deaths": "d", "recovered": "r", "cases7d": "c7", "incidence7d": "i7"}, inplace=True)
    
    # for smaler files rename fields
    # i = Id(Landkreis or Bundesland)
    # t = Name of Id(Landkreis or Bundesland)
    # m = Meldedatum
    # c = cases
    # d = deaths
    # r = recovered
    # c7 = cases7d (cases7days)
    # i7 = incidence7d (incidence7days)

    #convert dates to following numbers since 2020-01-01 to hold the files as short as possible
    LK["m"] = (pd.to_datetime(LK['m'], format='%Y-%m-%d') - pd.to_datetime('2020-01-01')).dt.days
    BL["m"] = (pd.to_datetime(BL['m'], format='%Y-%m-%d') - pd.to_datetime('2020-01-01')).dt.days

    # df2 = df1[['A', 'C']].copy()
    # split LK
    LKcases = LK[["i", "m", "c"]].copy()
    LKdeaths = LK[["i", "m", "d"]].copy()
    LKrecovered = LK[["i", "m", "r"]].copy()
    LKincidence = LK[["i", "m", "c7", "i7"]].copy()
        
    # split BL
    BLcases = BL[["i", "m", "c"]].copy()
    BLdeaths = BL[["i", "m", "d"]].copy()
    BLrecovered = BL[["i", "m", "r"]].copy()
    BLincidence = BL[["i", "m", "c7", "i7"]].copy()
        
    LKcasesFull = os.path.join(base_path, "..", "dataStore", "history", "cases", "districts.feather")
    LKdeathsFull = os.path.join(base_path, "..", "dataStore", "history", "deaths", "districts.feather")
    LKrecoveredFull = os.path.join(base_path, "..", "dataStore", "history", "recovered", "districts.feather")
    LKincidenceFull = os.path.join(base_path, "..", "dataStore", "history", "incidence", "districts.feather")
    
    BLcasesFull = os.path.join(base_path, "..", "dataStore", "history", "cases", "states.feather")
    BLdeathsFull = os.path.join(base_path, "..", "dataStore", "history", "deaths", "states.feather")
    BLrecoveredFull = os.path.join(base_path, "..", "dataStore", "history", "recovered", "states.feather")
    BLincidenceFull = os.path.join(base_path, "..", "dataStore", "history", "incidence", "states.feather")

    # read oldLK(cases, deaths, recovered, incidence) if old file exist
    # write new data 
    if os.path.exists(LKcasesFull):
        oldLKcases = ut.read_file(fn=LKcasesFull)
        ut.write_file(df=LKcases, fn=LKcasesFull, compression="lz4")
        LKDiffCases = ut.get_different_rows(oldLKcases, LKcases)
        LKDiffCases.set_index(["i", "m"], inplace=True, drop=False)
        oldLKcases.set_index(["i", "m"], inplace=True, drop=False)
        LKDiffCases["dc"] = LKDiffCases["c"] - oldLKcases["c"]
        LKDiffCases["dc"] = LKDiffCases["dc"].fillna(LKDiffCases["c"])
    else:
        ut.write_file(df=LKcases, fn=LKcasesFull, compression="lz4")
        LKDiffCases = LKcases.copy()
        LKDiffCases["dc"] = LKDiffCases["c"]

    if os.path.exists(LKdeathsFull):
        oldLKdeaths = ut.read_file(fn=LKdeathsFull)
        ut.write_file(df=LKdeaths, fn=LKdeathsFull, compression="lz4")
        LKDiffDeaths = ut.get_different_rows(oldLKdeaths, LKdeaths)
    else:
        ut.write_file(df=LKdeaths, fn=LKdeathsFull, compression="lz4")
        LKDiffDeaths = LKdeaths.copy()

    if os.path.exists(LKrecoveredFull):
        oldLKrecovered = ut.read_file(fn=LKrecoveredFull)
        ut.write_file(df=LKrecovered, fn=LKrecoveredFull, compression="lz4")
        LKDiffRecovered = ut.get_different_rows(oldLKrecovered, LKrecovered)
    else:
        ut.write_file(df=LKrecovered, fn=LKrecoveredFull, compression="lz4")
        LKDiffRecovered = LKrecovered.copy()

    if os.path.exists(LKincidenceFull):
        oldLKincidence = ut.read_file(fn=LKincidenceFull)
        ut.write_file(df=LKincidence, fn=LKincidenceFull, compression="lz4")
        # dont compare float values
        oldLKincidence.drop("i7", inplace=True, axis=1)
        temp = LKincidence.copy()
        LKincidence.drop("i7", inplace=True, axis=1)
        LKDiffIncidence = ut.get_different_rows(oldLKincidence, LKincidence)
        LKDiffIncidence.set_index(["i","m"], inplace=True, drop=False)
        temp.set_index(["i","m"], inplace=True, drop=True)
        LKDiffIncidence["i7"] = temp["i7"]
        LKDiffIncidence.reset_index(inplace=True, drop=True)
    else:
        ut.write_file(df=LKincidence, fn=LKincidenceFull, compression="lz4")
        LKDiffIncidence = LKincidence.copy()
    
    # read oldBL(cases, deaths, recovered, incidence) if old file exist
    # write new data
    if os.path.exists(BLcasesFull):
        oldBLcases = ut.read_file(fn=BLcasesFull)
        ut.write_file(df=BLcases, fn= BLcasesFull, compression="lz4")
        BLDiffCases = ut.get_different_rows(oldBLcases, BLcases)
        BLDiffCases.set_index(["i", "m"], inplace=True, drop=False)
        oldBLcases.set_index(["i","m"], inplace=True, drop=False)
        BLDiffCases["dc"] = BLDiffCases["c"] - oldBLcases["c"]
        BLDiffCases["dc"] = BLDiffCases["dc"].fillna(BLDiffCases["c"])
    else:
        ut.write_file(df=BLcases, fn= BLcasesFull, compression="lz4")
        BLDiffCases = BLcases.copy()
        BLDiffCases["dc"] = BLDiffCases["c"]
        
    if os.path.exists(BLdeathsFull):
        oldBLdeaths = ut.read_file(fn=BLdeathsFull)
        ut.write_file(df=BLdeaths, fn=BLdeathsFull, compression="lz4")
        BLDiffDeaths = ut.get_different_rows(oldBLdeaths, BLdeaths)
    else:
        ut.write_file(df=BLdeaths, fn=BLdeathsFull, compression="lz4")
        BLDiffDeaths = BLdeaths.copy()
    
    if os.path.exists(BLrecoveredFull):
        oldBLrecovered = ut.read_file(fn=BLrecoveredFull)
        ut.write_file(df=BLrecovered, fn=BLrecoveredFull, compression="lz4")
        BLDiffRecovered = ut.get_different_rows(oldBLrecovered, BLrecovered)
    else:
        ut.write_file(df=BLrecovered, fn=BLrecoveredFull, compression="lz4")
        BLDiffRecovered = BLrecovered.copy()
    
    if os.path.exists(BLincidenceFull):
        oldBLincidence = ut.read_file(fn=BLincidenceFull)
        ut.write_file(df=BLincidence, fn=BLincidenceFull, compression="lz4")
        oldBLincidence.drop("i7", inplace=True, axis=1)
        temp = BLincidence.copy()
        BLincidence.drop("i7", inplace=True, axis=1)
        BLDiffIncidence = ut.get_different_rows(oldBLincidence, BLincidence)
        BLDiffIncidence.set_index(["i","m"], inplace=True, drop=False)
        temp.set_index(["i","m"], inplace=True, drop=True)
        BLDiffIncidence["i7"] = temp["i7"]
        BLDiffIncidence.reset_index(inplace=True, drop=True)
    else:
        ut.write_file(df=BLincidence, fn=BLincidenceFull, compression="lz4")
        BLDiffIncidence = BLincidence.copy()
    
    # calculate diff data
    ChangeDateInt = (Datenstand - pd.to_datetime('2020-01-01')).days
    LKDiffCases["cD"] = ChangeDateInt
    LKDiffDeaths["cD"] = ChangeDateInt
    LKDiffRecovered["cD"] = ChangeDateInt
    LKDiffIncidence["cD"] = ChangeDateInt
    
    BLDiffCases["cD"] = ChangeDateInt
    BLDiffDeaths["cD"] = ChangeDateInt
    BLDiffRecovered["cD"] = ChangeDateInt
    BLDiffIncidence["cD"] = ChangeDateInt
    
    LKDiffCasesFull = os.path.join(base_path, "..", "dataStore", "historychanges", "cases", "districts_Diff.csv")
    LKDiffDeathsFull = os.path.join(base_path, "..", "dataStore", "historychanges", "deaths", "districts_Diff.csv")
    LKDiffRecoveredFull = os.path.join(base_path, "..", "dataStore", "historychanges", "recovered", "districts_Diff.csv")
    LKDiffIncidenceFull = os.path.join(base_path, "..", "dataStore", "historychanges", "incidence", "districts_Diff.csv")

    BLDiffCasesFull = os.path.join(base_path, "..", "dataStore", "historychanges", "cases", "states_Diff.csv")
    BLDiffDeathsFull = os.path.join(base_path, "..", "dataStore", "historychanges", "deaths", "states_Diff.csv")
    BLDiffRecoveredFull = os.path.join(base_path, "..", "dataStore", "historychanges", "recovered", "states_Diff.csv")
    BLDiffIncidenceFull = os.path.join(base_path, "..", "dataStore", "historychanges", "incidence", "states_Diff.csv")
    
    LKDiffCases["dc"] = LKDiffCases["dc"].astype(int)
    LKDiffCases.reset_index(inplace=True, drop=True)
    if os.path.exists(LKDiffCasesFull):
        ut.write_csv(df=LKDiffCases, full_fn=LKDiffCasesFull, dtype=HCC_dtp, mode='a')
    else:
        ut.write_csv(df=LKDiffCases, full_fn=LKDiffCasesFull, dtype=HCC_dtp)

    if os.path.exists(LKDiffDeathsFull):
        ut.write_csv(df=LKDiffDeaths, full_fn=LKDiffDeathsFull, dtype=HCD_dtp, mode='a')
    else:
        ut.write_csv(df=LKDiffDeaths, full_fn=LKDiffDeathsFull, dtype=HCD_dtp)

    if os.path.exists(LKDiffRecoveredFull):
        ut.write_csv(df=LKDiffRecovered, full_fn=LKDiffRecoveredFull, dtype=HCR_dtp, mode='a')
    else:
        ut.write_csv(df=LKDiffRecovered, full_fn=LKDiffRecoveredFull, dtype=HCR_dtp)

    if os.path.exists(LKDiffIncidenceFull):
        ut.write_csv(df=LKDiffIncidence, full_fn=LKDiffIncidenceFull, dtype=HCI_dtp, mode='a')
    else:
        ut.write_csv(df=LKDiffIncidence, full_fn=LKDiffIncidenceFull, dtype=HCI_dtp)
    
    BLDiffCases["dc"] = BLDiffCases["dc"].astype(int)
    BLDiffCases.reset_index(inplace=True, drop=True)
    if os.path.exists(BLDiffCasesFull):
        ut.write_csv(df=BLDiffCases, full_fn=BLDiffCasesFull, dtype=HCC_dtp, mode='a')
    else:
        ut.write_csv(df=BLDiffCases, full_fn=BLDiffCasesFull, dtype=HCC_dtp)

    if os.path.exists(BLDiffDeathsFull):
        ut.write_csv(df=BLDiffDeaths, full_fn=BLDiffDeathsFull, dtype=HCD_dtp, mode='a')
    else:
        ut.write_csv(df=BLDiffDeaths, full_fn=BLDiffDeathsFull, dtype=HCD_dtp)

    if os.path.exists(BLDiffRecoveredFull):
        ut.write_csv(df=BLDiffRecovered, full_fn=BLDiffRecoveredFull, dtype=HCR_dtp, mode='a')
    else:
        ut.write_csv(df=BLDiffRecovered, full_fn=BLDiffRecoveredFull, dtype=HCR_dtp)
    
    if os.path.exists(BLDiffIncidenceFull):
        ut.write_csv(df=BLDiffIncidence, full_fn=BLDiffIncidenceFull, dtype=HCI_dtp, mode='a')
    else:
        ut.write_csv(df=BLDiffIncidence, full_fn=BLDiffIncidenceFull, dtype=HCI_dtp)
    
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

    # load covid latest from web
    Datenstand = dt.datetime.fromtimestamp(meta["modified"] / 1000)
    Datenstand = Datenstand.replace(hour=0, minute=0, second=0, microsecond=0)
    LK = pd.read_csv(meta["filepath"], engine="pyarrow", usecols=CV_dtypes.keys(), dtype=CV_dtypes)
        
    # ----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
    LK = ut.squeeze_dataframe(LK)
    
    # History
    # used keylists
    key_list_LK = ["i", "m"]
    key_list_BL = ["i", "m"]
    key_list_ID0 = ["m"]

    LK["AnzahlFall"] = np.where(LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0).astype(int)
    LK["AnzahlTodesfall"] = np.where(LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0).astype(int)
    LK["AnzahlGenesen"] = np.where(LK["NeuGenesen"].isin([1, 0, -9]), LK["AnzahlGenesen"], 0).astype(int)
    LK.drop(["NeuerFall", "NeuerTodesfall", "NeuGenesen"], inplace=True, axis=1)
    LK.rename(columns={"IdLandkreis": "i", "Meldedatum": "m", "AnzahlFall": "c", "AnzahlTodesfall": "d", "AnzahlGenesen": "r"}, inplace=True)
    agg_key = {
        c: "max" if c in ["i"] else "sum"
        for c in LK.columns
        if c not in key_list_LK
    }
    LK = LK.groupby(by=key_list_LK, as_index=False, observed=True).agg(agg_key)
    
    LK["i"] = LK['i'].map('{:0>5}'.format)
    LK = ut.squeeze_dataframe(LK)
            
    BL = LK.copy()
    BL["i"] = BL["i"].str.slice(0,2)
    BL = ut.squeeze_dataframe(BL)
    agg_key = {
        c: "max" if c in ["i"] else "sum"
        for c in BL.columns
        if c not in key_list_BL
    }
    BL = BL.groupby(by=key_list_BL, as_index=False, observed=True).agg(agg_key)
    BL = ut.squeeze_dataframe(BL)
          
    agg_key = {
        c: "max" if c in ["i"] else "sum"
        for c in BL.columns
        if c not in key_list_ID0
    }
    ID0 = BL.groupby(by=key_list_ID0, as_index=False, observed=True).agg(agg_key)
    ID0["i"] = "00"
    BL = pd.concat([ID0, BL])
    
    BL["m"] = BL["m"].astype(str)
    LK["m"] = LK["m"].astype(str)
        
    # fill dates for every region
    allDates = ut.squeeze_dataframe(pd.DataFrame(pd.date_range(end=(Datenstand - dt.timedelta(days=1)), start="2019-12-26").astype(str), columns=["m"]))
    # add Einwohner
    BL_BV = BV[((BV["AGS"].isin(BL["i"])) & (BV["GueltigAb"] <= Datenstand) & (BV["GueltigBis"] >= Datenstand) & (BV["Altersgruppe"] == "A00+") & (BV["AGS"].str.len() == 2))].copy()
    BL_BV.drop(["GueltigAb", "GueltigBis", "Altersgruppe", "männlich", "weiblich", "Name"], inplace=True, axis=1)
    BL_BV.rename(columns={"AGS": "i"}, inplace=True)

    LK_BV = BV[((BV["AGS"].isin(LK["i"])) & (BV["GueltigAb"] <= Datenstand) & (BV["GueltigBis"] >= Datenstand) & (BV["Altersgruppe"] == "A00+") & (BV["AGS"].str.len() == 5))].copy()
    LK_BV.drop(["GueltigAb", "GueltigBis", "Altersgruppe", "männlich", "weiblich"], inplace=True, axis=1)
    LK_BV.rename(columns={"AGS": "i", "Name": "Landkreis"}, inplace=True)

    BLDates = ut.squeeze_dataframe(BL_BV.merge(allDates, how="cross"))
    LKDates = ut.squeeze_dataframe(LK_BV.merge(allDates, how="cross"))
    
    BL = BL.merge(BLDates, how="right", on=["i", "m"])
    LK = LK.merge(LKDates, how="right", on=["i", "m"])
    LK = LK[LK["Landkreis"].notna()]
    LK.drop(["Landkreis"], inplace=True, axis=1)

    #fill nan with 0
    BL["c"] = BL["c"].fillna(0).astype(int)
    BL["d"] = BL["d"].fillna(0).astype(int)
    BL["r"] = BL["r"].fillna(0).astype(int)

    LK["c"] = LK["c"].fillna(0).astype(int)
    LK["d"] = LK["d"].fillna(0).astype(int)
    LK["r"] = LK["r"].fillna(0).astype(int)
    
    BL["m"] = BL["m"].astype(str)
    BL = BL.groupby(["i"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    BL.reset_index(inplace=True, drop=True)
    BL.sort_values(["i", "m"], inplace=True, axis=0)
    BL["i7"] = (BL["c7"] / BL["Einwohner"] * 100000).round(5)
    BL.drop(["Einwohner"], inplace=True, axis=1)
        
    LK["m"] = LK["m"].astype(str)
    LK = LK.groupby(["i"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    LK.reset_index(inplace=True, drop=True)
    LK.sort_values(["i", "m"], inplace=True, axis=0)
    LK["i7"] = (LK["c7"] / LK["Einwohner"] * 100000).round(5)
    LK.drop(["Einwohner"], inplace=True, axis=1)
    
    update(meta=meta, BL=BL, LK=LK, mode="init")
    
    return
