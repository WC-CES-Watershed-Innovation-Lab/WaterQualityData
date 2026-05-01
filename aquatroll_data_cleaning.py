# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 14:04:05 2026

@author: GIS
"""

import glob
import os
import pandas as pd

location_ids = {
    "Lower Langford Creek AquaTroll" : 4840973161857024,
    "Radcliffe Outflow AquaTroll" : 5276098860482560,
    "Millington AquaTroll" : 5687072567394304,
    "SE Creek AquaTroll" : 6000235094540288,
    "Shipyard Landing Dock AquaTroll" : 6228783747956736,
    "Upper East Langford Dock AquaTroll" : 6235146771365888,
    "Morgan Creek AquaTroll" : 6265987319005184    
}

# timestamps allow us to connect data rows across csvs
# depth_outlier_timestamps grabs all of the timestamps where depth is not logical
def depth_outlier_timestamps(filename, df):
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    outlier_times = df[(df['value'] < 0)]['timestamp'].tolist() 
    print(outlier_times)
    return outlier_times

#cycles through all the csvs from input location
def clean_csv(loc):
    #call each csv from folder
    folder_path = f"C:\\Users\\GIS\\MichaelHudak projects\\test_data_cleaning\\{loc}"
    all_files = glob.glob(os.path.join(folder_path, "*.csv"))
    for filename in all_files:
        df = pd.read_csv(filename, header=0)
        print(f"Original size: {df.shape}")

        #identify datetimes to throw out & remove from all csvs from the location
        if filename == f"C:\\Users\\GIS\\MichaelHudak projects\\test_data_cleaning\\{loc}\\Depth.csv": 
            outlier_times = depth_outlier_timestamps(filename, df)
        print(f"New size: {df.shape}")
        
        #remove outlier times from all csvs
    for filename in all_files:
        df = pd.read_csv(filename, header=0)
        df = df[~df['timestamp'].isin(outlier_times)]
        
        #remove duplicates
        df.drop_duplicates(inplace=True)
        #update csv with cleaned data
        df.to_csv(f'{filename}', index=False)
        
        # Delete the file if cleaning left it empty
        if df.empty:
            os.remove(filename) # if this deletes Depth.csv, the program run will crash
            print(f"Deleted empty file after cleaning: {filename}")
        
    return
            
#start with copies of each csv named _clean and then override each time it's cleaned     

for loc in location_ids:
    clean_csv(loc)
    
