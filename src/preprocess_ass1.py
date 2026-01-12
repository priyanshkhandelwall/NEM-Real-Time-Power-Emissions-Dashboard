"""
Preprocess Assignment 1 nger_merged.csv for integration.
Run once: python preprocess_ass1.py
"""
import pandas as pd

# Load your nger_merged.csv (historical emissions)
df = pd.read_csv("nger_merged.csv")

# Rename to match Ass2 schema
df = df.rename(columns={
    "facilityname": "name",
    "state": "region",
    "primaryfuel": "fuel_type",
    "reporting_year": "year",
    "electricityproductionmwh": "power_mwh_yearly",
    "totalemissionstco2e": "emissions_tco2e_yearly"
})

# DUID mapping (expanded for your file; add more if needed)
name_to_duid = {
    "Gunning Wind Farm": "GUNNING1",
    "Waubra Wind Farm": "WAUBRA1",
    "Banimboola Hydro": "BANIMB1",
    "Bogong Power Station": "BOGONG1",
    "Burrendong Hydro": "BURR1",
    "Cairn Curran Hydro": "CAIRN1",
    "Clover Hydro": "CLOVER1",
    "Coopers Cogeneration": "COOPER1",
    "Copeton Hydro": "COPET1",
    "Dartmouth Hydro": "DARTM1",
    "Eildon Hydro": "EILDON1",
    "Glenbawn Hydro": "GLENB1",
    "Glenorchy Landfill Generation": "GLENLF1",
    "Wandoan Solar Farm 1": "WANDOAN1",
    "Rocky Point Sugar Mill Cogeneration": "ROCKY1",
    "Cocos Keeling Islands - Home Island Generation": "CKHOME1",
    "Cocos Keeling Islands - West Island Generation": "CKWEST1",
    "Weipa Solar Power Station": "WEIPA1",
    "West Wyalong SF": "WESTWY1",
    "White Rock Solar Farm Pty Ltd": "WHROCK1",
    "Wild Cattle Hill Wind Farm": "WCHILL1",
    "Willogoleche Wind Farm": "WILLOG1",
    "Winton Solar Farm": "WINTON1",
    "Glenrowan West Solar Farm": "GLENRW1",
    "Woolooga SF": "WOOLOO1",
    "YATPOOL SOLAR FARM": "YATPOOL1",
    # Add from Data Augmentation.csv if merging
    "Laura Johnson Home, Townview - Solar w SGU - QLD": "LJHTOWN1"
}
df["duid"] = df["name"].map(name_to_duid).fillna("UNKNOWN")

# Drop unmatched, keep core
df = df[df["duid"] != "UNKNOWN"][["duid", "name", "region", "fuel_type", "year", "power_mwh_yearly", "emissions_tco2e_yearly"]]

df.to_csv("assignment1_data.csv", index=False)
print(f"Ass1 CSV ready! {len(df)} facilities with DUIDs.")