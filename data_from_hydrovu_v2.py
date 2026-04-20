#!/usr/bin/env python
# coding: utf-8

"""
HydroVu API documentation: https://www.hydrovu.com/public-api/docs/index.html
"""

# In[1]:


import pandas as pd # Dataframes, reads CSVs into dataframes
import matplotlib.pyplot as plt # Available for quick plotting
import requests # Connects with HydroVu and GitHub for data calls and uploads
from datetime import datetime, timedelta # Converts epoch time from raw data, sets start date when building CSVs
import plotly.express as px # Interactive graph display that gets sent to GitHub
import plotly.io as pio
import os # Enables viewing external files
import glob # Enables grabbing all files in a folder
#import sys 
import time # Tracks code runtime and prints at the end of run
import base64 # Encoding necessary to upload html to GitHub
from io import BytesIO, StringIO # Enables treating a string like a file object for GitHub upload

start_time = time.time()
#sys.setrecursionlimit(10000) # Increase the limit to 10000
pio.renderers.default = 'browser' #determines how plot displays


# ## Prologue

# In[81]:


# These must be defined before running the code
LOCAL_CLIENT_ID = "mhudak_" # found in HydroVu website
LOCAL_CLIENT_SECRET = "" # found in HydroVu website
# git token must be generated in GitHub
LOCAL_GIT_TOKEN = ""

# HydroVu endpoints are necessary for calling data from HydroVu, found in HydrVu API docs
LOCAL_LOCATIONS_ENDPOINT = "https://hydrovu.com/public-api/v1/locations/list"
LOCAL_OAUTH_ENDPOINT     = "https://hydrovu.com/public-api/oauth/token"
LOCAL_DATA_ENDPOINT      = "https://hydrovu.com/public-api/v1/locations/"


# In[7]:

# Location IDs dict enables easy transition between site codes and readable site names
location_ids = {
    "Lower Langford Creek AquaTroll" : 4840973161857024,
    "Radcliffe Outflow AquaTroll" : 5276098860482560,
    "Millington AquaTroll" : 5687072567394304,
    "SE Creek AquaTroll" : 6000235094540288,
    "Shipyard Landing Dock AquaTroll" : 6228783747956736,
    "Upper East Langford Dock AquaTroll" : 6235146771365888,
    "Morgan Creek AquaTroll" : 6265987319005184
    
}

ALL_PARAMS = ["Actual Conductivity", "Specific Conductivity", "Salinity", "Resistivity",
              "Density", "Total Dissolved Solids", "Chl-a Fluorescence", "Chl-a Concentration",
              "Turbidity", "Total Suspended Solids", "Temperature", "External Voltage", "Pressure",
              "Depth", "pH", "pH MV", "ORP", "DO", "% Saturation O₂", "Partial Pressure O₂",
              "Level Depth to Water", "Level Elevation", "Baro"]

UNITS_BY_PARAM = {
    "Actual Conductivity" : 'µS/cm', 
    "Specific Conductivity" : 'µS/cm', 
    "Salinity" : 'psu', 
    "Resistivity" : 'Ω-cm',
    "Density" : 'g/cm³', 
    "Total Dissolved Solids" : 'mg/L', 
    "Chl-a Fluorescence" : 'RFU',
    "Chl-a Concentration" : 'mg/L',
    "Turbidity" : 'NTU', 
    "Total Suspended Solids" : 'mg/L', 
    "Temperature" : 'C', 
    "External Voltage" : 'V', 
    "Pressure" : 'psi',
    "Depth" : 'm', 
    "pH" : 'pH', 
    "pH MV" : 'V', 
    "ORP" : 'V', 
    "DO" : 'mg/L', 
    "% Saturation O₂" : '% sat', 
    "Partial Pressure O₂" : 'psi',
    "Level Depth to Water" : 'm', 
    "Level Elevation" : 'm', 
    "Baro" : 'psi'
    }

# In[8]:

# Establish parameter and unit dicts to join friendly names with HydroVu codes
# HydroVu uses numeric codes for parameters and units, 
# so these dictionaries help convert codes into meaningful names
parameter_df = pd.read_csv("C:/Users/GIS/MichaelHudak projects/WIL monitor locations - parameter IDs.csv", header=0)
parameter_dict = {row["key_col"]:row["value_col"] for i, row in parameter_df.iterrows()}


unit_df = pd.read_csv("C:/Users/GIS/MichaelHudak projects/WIL monitor locations - unit IDs.csv")
unit_dict = {row["key_col"]:row["value_col"] for i, row in unit_df.iterrows()}



# In[11]:
"""
Use HydroVu client info to get a temporary access token.
HydroVu access token expire relatively quickly, so this should be updated with 
each run.
"""
def update_access_token():
    headers_for_auth = {#headers to get an authentication token
        "grant_type" : "client_credentials",
        "client_id" : LOCAL_CLIENT_ID,
        "client_secret" : LOCAL_CLIENT_SECRET
        }
    
    response = requests.post(url=LOCAL_OAUTH_ENDPOINT, data=headers_for_auth)
    response.raise_for_status()
    
    tokens = response.json()
    return(tokens["access_token"])


#access_token = update_access_token()

#Sets up authentication header using access token
headers_for_data = {
    "Authorization" : f"Bearer {update_access_token()}",
    "User-Agent" : LOCAL_CLIENT_ID
    }


# In[15]:


# Only run this to update the dictionary of locations
# This only gets first ten locations; use the web interface api instead

def get_locations():
    response = requests.get(url=LOCAL_LOCATIONS_ENDPOINT, headers=headers_for_data)
    response.raise_for_status()
    
    locations = response.json()
    return locations


# In[17]:
"""
Calculates the epoch time of a past date, 
set with an input of the number of days into the past.
Returns two epoch times: the present epoch time, and the epoch time of the designated past day
"""
def get_dates(days_ago):
    date_now   = datetime.now()
    now_epoch  = date_now.timestamp()
    past_date  = date_now - timedelta(days=days_ago)
    past_epoch = past_date.timestamp()
    return int(now_epoch), int(past_epoch)


# In[18]:

# Makes a single API call for a given location with parameters(page, date)
def make_one_call(desired_location, header_parameters):
    # Constructs a unique url using the LOCAL_DATA_ENDPOINT variable and desired_location input
    response = requests.get(url=f"{LOCAL_DATA_ENDPOINT}{location_ids[desired_location]}/data",
                            headers=headers_for_data, params=header_parameters)
    
    # Instead of raising an error, the function returns just a "null" value,
    # which the dependent functions can handle
    if response.ok:
        return response
    else:
        return "null"
    
    #data contains timestamp/value pairs for each parameterId, and each parameter has a unitId


# In[19]:
"""
The following functions are all very interconnected: loop_by_date(), extract_param_data(),
process_responses(), and merge_dfs(). First, loop_by_date() iterates through all of the data in
our specified date range. loop_by_date() stores all of the API call results in the response_list. 
So, the response_list is a list of complex dictionary items that contain timestamp and value pairs
for each parameter, but each dictionary is organized by timestamp and contains accessory information.
Then, extract_param_data() sorts through the complex response_list and organizes a new dictionary, results,
into parameter dataframes by page. (Remember, each page is only about 120 datapoints, which spans about
2 days. Response will be a list of dictionaries, where there is a dictionary for each data page, and
each dictionary consists of parameter_id code keys matched with organized, 1-parameter dataframe values.)
process_responses() runs the previous two functions in a logically-sound way. Finally, merge_dfs takes the 
list of page-separated parameter dictionaries and merges them into full parameter dataframes. That is, 
the final output includes the combined_dfs dictionary, which for the given location over our specified
time frame, consists of all paramaters with each parameter in its own singular dataframe.
"""

# Establishes logic to make continuous HydroVu API calls by looping through data date-wise
# HydroVu only returns about 120 data points with each request, so we need to loop multiple requests
def loop_by_date(desired_location, now_date, start_date):

    response_list = []
    checked_dates = [] # Anti infinite loop control
    while start_date < now_date: # start_date must be some epoch date in the past
        #print("start_date: ", datetime.fromtimestamp(start_date))
        header_parameters = {
            "startTime" : start_date, 
        }
        r = make_one_call(desired_location, header_parameters)
        if r == "null": # if the make_one_call() does not return any data, stop looping
            break
        response_list.append(r)
        checked_dates.append(start_date)
        
        response_data = r.json()
        end_date = response_data["parameters"][0]["readings"][-1]["timestamp"]
        
        start_date = end_date
        if start_date in checked_dates: # if the next loop would check a date that we've already checked
            break

    return response_list

# In[20]:


# response.json as input and returns a dictionary of all the parameters in the response.json
# outputs parameter dict with paramterID (pid) key and dataframe value
def extract_param_data(loc_data):
    loc_id = loc_data["locationId"]
    results = {}
    
    for param in loc_data["parameters"]: # Loops through all parameters
        pid = param["parameterId"]
        
        # Creates a dataframe based on the timestamp-value pairs for each parameter
        df = pd.DataFrame(param["readings"]) 
        
        df["param_name"] = parameter_dict[pid]
        df["unit_name"] = unit_dict[param["unitId"]]
        df["locationId"] = loc_id
        
        results[pid] = df
    return results


# In[21]:


# input is a list of all API call responses for a location (so we can get multiple pages of data)
# output is a list of all paramter dataframes, separated by API call page 
# list of dicts, where each dict is all the parameters from an individual API call page
def process_responses(response_list):
    dict_list = []
    for r in response_list:
        response_data = r.json()
        data_per_response = extract_param_data(response_data)
        dict_list.append(data_per_response)
    return dict_list




# In[23]:


# Takes the list of separated dictionaries as input
# Creates a key list based on all of the keys in the first dictionary
# Concatenates dataframes based on matching key values
# Returns the list of keys and the dictionary of dfs; use the key list to access the combined_dfs
def merge_dfs(dict_list):
    key_list = []
    for d in dict_list: # Runs through all dictionaries and grabs their keys
        dict_keys = d.keys() # Creates a list object with keys
        for key in dict_keys:
            key_list.append(key) # Breaks up the dict_keys list into individual keys
   
    key_list = set(key_list) # Gets only unique values in the list
    
    combined_dfs = {}
    
    for key in key_list: # Loops through each key and concats dicts key-wise
        dfs_to_concat = []
        for d in dict_list:
            if key in d:
                dfs_to_concat.append(d[key])
        
        if dfs_to_concat:
            combined_dfs[key] = pd.concat(dfs_to_concat, ignore_index=True)
        
    return list(key_list), combined_dfs


# In[24]:

    
# Run this to build a csv for a location that does not have a csv yet
def build_csv(loc, how_many_days_ago): # how many days in the past should we grab data for
    date_now, date_past = get_dates(how_many_days_ago) # gets actual date values
    responses = loop_by_date(loc, date_now, date_past)
    response_dict_list = process_responses(responses) # Returns a blank list, [], if the site has no data w/in date range
    if response_dict_list != []: # if the response_dict_list is not empty
        keys, loc_dfs = merge_dfs(response_dict_list)
        #print(loc_dfs)
        for df in loc_dfs.values(): # Converts each df to csv based on unique path link
            df.to_csv(f"C:\\Users\\GIS\\MichaelHudak projects\\HydroVu_Location_Params\\{loc}\\{df.iloc[0,2]}.csv")
    else:
        print(f"No data in timeframe for {loc}")
        

# In[]:
    
# Updates an existing set of csvs for a location that already has csvs
def update_csv(loc):
    # Establishes a unique folder_path for the location folder
    folder_path = f"C:\\Users\\GIS\\MichaelHudak projects\\HydroVu_Location_Params\\{loc}"
    # grab all files in that folder that end with .csv
    all_files = glob.glob(os.path.join(folder_path, "*.csv")) 
    final_date_list = []
    for filename in all_files:
        df = pd.read_csv(filename, header=0)
        last_date = df.iloc[-1, 1]
        final_date_list.append(last_date) # List of most recent dates for each parameter
        
    most_recent_date = max(final_date_list) # The most recent data across all parameters
    responses = loop_by_date(loc, datetime.now().timestamp(), most_recent_date) 
    response_dict_list = process_responses(responses)
    keys, loc_dfs = merge_dfs(response_dict_list)
    
    # Appends (mode='a') new data to the existing csv
    for df in loc_dfs.values():
        df.to_csv(f"{folder_path}\\{df.iloc[0,2]}.csv", mode='a', index=True, header=False)
        



# In[26]:
"""
Takes a location name as input and creates a folder_path link for that location.
Uses a try/except pairing so that the code doesn't crash if a location folder does not exist yet.
Grabs all the .csv files from the folder path, coverts each one to a dataframe, and returns 
a list of all the dataframes.
"""
def dfs_from_csvs(loc):
    # Creates a path to the location folder, which is flexible to the location input
    folder_path = f"C:\\Users\\GIS\\MichaelHudak projects\\HydroVu_Location_Params\\{loc}"
    
    # If this function is run for a location without a folder, the function will jump to the except statement
    try: 
        all_files = glob.glob(os.path.join(folder_path, "*.csv")) # Grabs all .csv files in the folder_path
        df_list = []
        for filename in all_files:
            df = pd.read_csv(filename, header=0)
            df_list.append(df) # Stores all converted dataframes in a list
        return df_list
    except:
        print(f"{loc} csvs do not exist")

# ## 4. Make the plots (pyplot & plotly)

# In[39]:

# def make_graph(df, title, unit):
#     plt.scatter(df.timestamp, df.value, color='black', marker='o')
    
#     plt.title(f"{title}")

#     plt.ylabel(unit)
    
#     plt.show()


# In[40]:

"""
plotly_graph is not called in the typical script run, HOWEVER
if you want to experiment with graph appearance, use this for convenience
"""    

def plotly_graph(df, loc, param, unit):
    fig = px.scatter(x=df['timestamp'], y=df["value"],
                 labels={'x': "Time",
                         'y': f"{loc} {param} ({unit})"})

    #shows interactive plot in browser
    #fig.show()
    
    fig.write_html(f"C:/Users/GIS/MichaelHudak projects/hub_site_data/AquaTroll Graphs/{loc}/{param}",
                   include_plotlyjs='cdn')#make a folder for each location
    



# In[]: Experimenting with Plotly graphs

def location_id_to_name(df):
    id_num = df['locationId'][0]
    reversed_loc_dict = {value: key for key, value in location_ids.items()}
    df['locationId'] = df['locationId'].map(lambda x: reversed_loc_dict[x])
    return df

def all_site_plotly_graph(dfs, param):
    #big_df = pd.concat([sec_df, morg_df])
    plot_times = convert_dates(dfs['timestamp'])
    big_df = location_id_to_name(dfs)
    unit_label = UNITS_BY_PARAM[param]
    
    fig = px.line(big_df, x=plot_times, y='value', color='locationId',
                     title=f'{param} by Location',
                     labels={'locationId': "Location",
                             'x': "Date/Time",
                             'value': f"{param}, ({unit_label})"})
    
    return fig, param





# In[48]:


#Ensure access token is updated and active
#access_token = update_access_token()


# In[63]:

# Path Parameter constants
OWNER = "WC-CES-Watershed-Innovation-Lab"
REPO  = "WaterQualityData"


# The local_git_token may need manual updates in GitHub
git_headers = {
    "Authorization" : f"token {LOCAL_GIT_TOKEN}",
    "Accept": "application/vnd.github+json",
    "User-Agent": "data_update"
}


# In[65]:


# Checks if file exists and returns necessary information
def file_exists(url):
    global git_headers
    
    meta = requests.get(url, headers=git_headers)
    if meta.status_code == 200:
        return meta.json()["sha"] # sha is important for GitHub access
    else:
        return None


# In[67]:

# Make the request to GitHub to alter graph files
def git_api_call(url, content):
    global git_headers

    sha = file_exists(url) # Checks if file exists before proceeding
    
    git_body_params = {
    "message" : "Updating the plots",
    "content" : content,
    }

    if sha is not None: 
        git_body_params["sha"] = sha
    
    # This should NOT be indented
    response = requests.put(url, headers=git_headers, json=git_body_params)
    response.raise_for_status()
    print(response.json())
    # else:
    #     print("sha is none")


# In[69]:

# Converts dates from epoch to normal human-friendly datetime
def convert_dates(time_array):
    new_dates = []
    for val in time_array:
        local_datetime = datetime.fromtimestamp(val)
        new_dates.append(local_datetime)
    return new_dates
    

# In[71]:


def plotly_bytes(df, loc, param, unit):
    #dates = convert_dates(df['timestamp'])
    #convert_times() function needs to be defined. Set x equal to dates
    x_times = convert_dates(df['timestamp'])
    
    fig = px.scatter(x=x_times, y=df["value"],
                 labels={'x': "Time",
                         'y': f"{loc} {param} {unit}"})
    #shows interactive plot in browser
    #fig.show()
    buf = StringIO()
    fig.write_html(buf, include_plotlyjs='cdn')

        
    html_text = buf.getvalue()

    content_base64 = base64.b64encode(html_text.encode("utf-8")).decode("utf-8")

    url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/docs/{loc}/{param}.html"

    git_api_call(url, content_base64)

# Similar to plotly_bytes above but does not require specified loc or unit
def all_locs_plotly_bytes(fig, plot_param):
    
    buf = StringIO()
    fig.write_html(buf, include_plotlyjs='cdn')

        
    html_text = buf.getvalue()

    content_base64 = base64.b64encode(html_text.encode("utf-8")).decode("utf-8")

    url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/docs/All%20Locations/{plot_param}.html"

    git_api_call(url, content_base64)



# In[]:

"""
Must uncomment for loop codes for full functionality.
 - change the number in the buil_csv loop decide how many days in the past to start the csv
 - only run build_csv for a new location, or if an old location's csv gets deleted
 - the update_csv loop must be run before the plotly_bytes loop can have any effect
"""    

# for loc in location_ids:
#     build_csv(loc, 500)

# for loc in location_ids:
#     update_csv(loc)

# for loc in location_ids:
#     dfs_to_convert = dfs_from_csvs(loc) # NoneType if no csv exists
#     if dfs_to_convert:
#         for df in dfs_to_convert:
#             param_name = df.iloc[0,3]
#             unit_name = df.iloc[0,4]
#             plotly_bytes(df, loc, param_name, unit_name)
            
all_loc_dfs = {}    
for loc in location_ids:
    list_name = f"{loc}_df_list"
    all_loc_dfs[list_name] = dfs_from_csvs(loc) # creates key:value pair with location name key and list of dataframes

# Establish the all-loc dfs per parameter
all_loc_param_dfs = {}
for param in ALL_PARAMS: # this is for sure an inefficient sorting algorithm
    param_df = pd.DataFrame() # creates a dataframe to store data across all locations for the given param
    for loc in all_loc_dfs: #steps through each location and grabs all of its dataframes
        df_list = all_loc_dfs[loc]
        
        # This double for-loop is inefficient and logically unnecessary, 
        # but grabbing values from the dataframe wasn't working how I expected
        for df in df_list: 
            if df['param_name'][0] == param: # pulls out param-specific df and concatenates to all-location df for that param
                param_df = pd.concat([param_df, df])
    plot_fig, plot_param = all_site_plotly_graph(param_df, param)
    all_locs_plotly_bytes(plot_fig, plot_param)
    all_loc_param_dfs[param] = param_df


print("--- %s seconds ---" % (time.time() - start_time))


#print(convert_dates([1770663600]))
