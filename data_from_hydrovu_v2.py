#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import matplotlib.pyplot as plt
import requests
from datetime import datetime, timedelta
import plotly.express as px
import plotly.io as pio
import os
import glob
import sys
import time
import base64
from io import BytesIO, StringIO

start_time = time.time()
sys.setrecursionlimit(10000) # Increase the limit to 10000
pio.renderers.default = 'browser' #determines how plot displays


# ## Prologue

# In[ ]:






# In[4]:


#%env


# In[81]:


# These must be defined before running the code
LOCAL_CLIENT_ID = ""
LOCAL_CLIENT_SECRET = ""
LOCAL_GIT_TOKEN = ""


# In[6]:


LOCAL_LOCATIONS_ENDPOINT = "https://hydrovu.com/public-api/v1/locations/list"
LOCAL_OAUTH_ENDPOINT     = "https://hydrovu.com/public-api/oauth/token"
LOCAL_DATA_ENDPOINT      = "https://hydrovu.com/public-api/v1/locations/"


# In[7]:


# Location IDs dict enables easy transition between site codes and readable site names
location_ids = {
    "Mary Bee Gaines Dock AquaTroll" : 4840973161857024,
    "Radcliffe Outflow AquaTroll" : 5276098860482560,
    "Millington AquaTroll" : 5687072567394304,
    "SE Creek AquaTroll" : 6000235094540288,
    "Shipyard Landing Dock AquaTroll" : 6228783747956736,
    "Upper East Langford Dock AquaTroll" : 6235146771365888,
    "Morgan Creek AquaTroll" : 6265987319005184
    
}


# In[8]:


#Establish parameter and unit dicts to join friendly names with HydroVu codes
parameter_df = pd.read_csv("C:/Users/GIS/MichaelHudak projects/WIL monitor locations - parameter IDs.csv", header=0)
parameter_dict = {row["key_col"]:row["value_col"] for i, row in parameter_df.iterrows()}


unit_df = pd.read_csv("C:/Users/GIS/MichaelHudak projects/WIL monitor locations - unit IDs.csv")
unit_dict = {row["key_col"]:row["value_col"] for i, row in unit_df.iterrows()}


# In[ ]:





# ## SECTION 1

# ## 1. Update Access Token

# In[11]:


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


# In[12]:


access_token = update_access_token()


# In[13]:


#Sets up authentication header
headers_for_data = {
    "Authorization" : f"Bearer {update_access_token()}",
    "User-Agent" : LOCAL_CLIENT_ID
    }


# ## 2. Get Active Locations

# In[15]:


#Only run this to update the dictionary of locations
#This only gets first ten locations; use the web interface api instead
def get_locations():
    response = requests.get(url=LOCAL_LOCATIONS_ENDPOINT, headers=headers_for_data)
    response.raise_for_status()
    
    locations = response.json()
    return locations


# ## 3. Get Data by Looping Dates

# In[17]:


# Calculates the epoch time of a past date, set the past date by adjusting days=
# Theoretically, enables us to get data between a past date and present, but HydroVu API isn't quite behaving that way
def get_dates(days_ago):
    date_now   = datetime.now()
    now_epoch  = date_now.timestamp()
    past_date  = date_now - timedelta(days=days_ago)
    past_epoch = past_date.timestamp()
    return int(now_epoch), int(past_epoch)


# In[18]:


# Makes a single API call for a given location with parameters(page, date)
def make_one_call(desired_location, parameters):
    response = requests.get(url=f"{LOCAL_DATA_ENDPOINT}{location_ids[desired_location]}/data",
                            headers=headers_for_data, params=parameters)
    if response.ok:
        return response
    else:
        return "null"
    #response.raise_for_status()

    #return response
    
    #data contains timestamp/value pairs for each parameterId, and each parameter has a unitId


# In[19]:


def loop_by_date(desired_location, now_date, start_date):
    #now_date, start_date = get_dates(num_days)

    response_list = []
    checked_dates = [] # Anti infinite loop control
    while start_date < now_date:
        #print("start_date: ", datetime.fromtimestamp(start_date))
        parameters = {
            "startTime" : start_date, 
        }
        r = make_one_call(desired_location, parameters)
        if r == "null":
            break
        response_list.append(r)
        checked_dates.append(start_date)
        
        response_data = r.json()
        end_date = response_data["parameters"][0]["readings"][-1]["timestamp"]
        
        start_date = end_date
        if start_date in checked_dates:
            break

    return response_list

        


# In[20]:


# response.json as input and returns a dictionary of all the parameters in the response.json
# outputs parameter dict with paramterID (pid) key and dataframe value
def extract_param_data(loc_data):
    loc_id = loc_data["locationId"]
    results = {}
    
    for param in loc_data["parameters"]:
        pid = param["parameterId"]

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


# In[22]:


# this is how combining dictionaries works
dict1 = {"A" : [2, 4, 6], "B" : [1, 3, 5]}
dict2 = {"A" : [10, 12], "B" : [11, 13]}

dict3 = {}
for key in dict1:
    dict3[key] = dict1[key] + dict2[key]

#print(dict3)


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


fake_list = [1, 1, 3, 4, 5, 6, 7, 7, 8, 9]
print(set(fake_list))

car = {
  "brand": "Ford",
  "model": "Mustang",
  "year": 1964
}

x = car.keys()
print(x)
for key in x:
    print(key)

# In[24]:

def build_csv(loc, how_many_days_ago):
    date_now, date_past = get_dates(how_many_days_ago)
    responses = loop_by_date(loc, date_now, date_past)
    response_dict_list = process_responses(responses) # Returns a blank list, [], if the site has no data w/in date range
    if response_dict_list != []:
        keys, loc_dfs = merge_dfs(response_dict_list)
        print(loc_dfs)
        for df in loc_dfs.values():
            df.to_csv(f"C:\\Users\\GIS\\MichaelHudak projects\\HydroVu_Location_Params\\{loc}\\{df.iloc[0,2]}.csv")
            #df.to_csv(r"C:\Users\GIS\MichaelHudak projects\HydroVu_Location_Params\Mary Bee Gaines Dock AquaTroll")
    else:
        print(f"No data in timeframe for {loc}")
        
#build_csv("Mary Bee Gaines Dock AquaTroll")


# In[]:
    
def update_csv(loc):
    folder_path = f"C:\\Users\\GIS\\MichaelHudak projects\\HydroVu_Location_Params\\{loc}"
    all_files = glob.glob(os.path.join(folder_path, "*.csv"))
    final_date_list = []
    for filename in all_files:
        df = pd.read_csv(filename, header=0)
        last_date = df.iloc[-1, 1]
        final_date_list.append(last_date)
        #print(last_date)
        
    most_recent_date = max(final_date_list)
    responses = loop_by_date(loc, datetime.now().timestamp(), most_recent_date) 
    response_dict_list = process_responses(responses)
    keys, loc_dfs = merge_dfs(response_dict_list)
    
    for df in loc_dfs.values():
        df.to_csv(f"{folder_path}\\{df.iloc[0,2]}.csv", mode='a', index=True, header=False)
        
update_csv("Mary Bee Gaines Dock AquaTroll")

# In[]:
    

# In[25]:

# for loc in location_ids:
#     build_csv(loc, 500)

# for loc in location_ids:
#     update_csv(loc)


# This code is great & functional for just getting the previous 2 weeks,
# but has nothing to do with the csv pathway
# loc_data_dict = {}
# for loc in location_ids:
#     responses = loop_by_date(loc, 14)
#     response_dict_list = process_responses(responses) # Returns a blank list, [], if the site has no data w/in date range
#     if response_dict_list == []:
#         continue
#     else:
#         keys, loc_dfs = merge_dfs(response_dict_list)
#         #
#         loc_data_dict.update({loc : loc_dfs})


# In[26]:

def dfs_from_csvs(loc):
    folder_path = f"C:\\Users\\GIS\\MichaelHudak projects\\HydroVu_Location_Params\\{loc}"
    try:
        all_files = glob.glob(os.path.join(folder_path, "*.csv"))
        df_list = []
        for filename in all_files:
            df = pd.read_csv(filename, header=0)
            df_list.append(df)
        return df_list
    except:
        print(f"{loc} csvs do not exist")


# In[27]:


# The make_one_call function breaks if we don't have recent data for the given location
#responses = loop_by_date("SE Creek AquaTroll")


# In[28]:


# for r in responses:
#     print(r.json()['parameters'][0]['readings'][0])
#     #print(r.headers['x-isi-next-page'])


#  x-isi-next-page: Z4dNGr+OlU+4zl1qrYX3RtxEYfDaH0ENGUQvYmR8Zy+jEWDh4Px7JHCLzLQhLd6X 
#x-isi-next-page: dxhaIG03uXWEe+mI14zqm/oacUhPQJXEaRsBO189afDxuM2tDjqGMYJ2F0qvclXs 
#x-isi-next-page: nF09MWMrxbASYxsCvJ6MVGjjwDAyrXtdoWBBAUXnENW2NZ5e31hXzKHAbODN2RiM 
# 
# 
# 

# In[30]:


# response_dict_list = process_responses(responses)
# print(response_dict_list)


# In[31]:


# keys, all_dfs = merge_dfs(response_dict_list)


# In[32]:


# print(keys)


# In[33]:


# # make_graph is defined later
# for key, value in all_dfs.items():
#     make_graph(value, value['param_name'][0], value['unit_name'][0]) 


# In[34]:


# store_api_results is not defined
# def get_combined_dfs():
#     global location_ids
#     for loc in location_ids:
#         responses = store_api_results("Millington AquaTroll")
#         response_dict_list = process_responses(responses)
#         keys, all_dfs = merge_dfs(response_dict_list)


# ## ^At this point, I have a dictionary of dataframes for a location

# In[36]:


#This code should be returning data (response.json())
# def get_data(desired_location):
#     #url is based on the desired location and requires {location_ids[location_name]}
#     data_page = ''
#     parameters = {
#     #"id" : location_ids[desired_location],
#     "startTime" : 1609477200, # This is the Epoch time for January 1st, 2021
#     "X-ISI-Start-Page" : data_page
#     }

    
#     response = requests.get(url=f"{LOCAL_DATA_ENDPOINT}{location_ids[desired_location]}/data",
#                             headers=headers_for_data, params=parameters)
#     response.raise_for_status()
#     data = response.json()
#     headers = response.headers
#     return(data) 
#     #data contains timestamp/value pairs for each parameterId, and each parameter has a unitId


# In[37]:


# print(get_data("Millington AquaTroll"))


# ## 4. Make the plots (pyplot & plotly)

# In[39]:


def make_graph(df, title, unit):
    plt.scatter(df.timestamp, df.value, color='black', marker='o')
    
    plt.title(f"{title}")

    plt.ylabel(unit)
    
    plt.show()


# In[40]:


def plotly_graph(df, loc, param, unit):
    fig = px.scatter(x=df['timestamp'], y=df["value"],
                 labels={'x': "Time",
                         'y': f"{loc} {param} {unit}"})

    #shows interactive plot in browser
    #fig.show()
    
    fig.write_html(f"C:/Users/GIS/MichaelHudak projects/hub_site_data/AquaTroll Graphs/{loc}/{param}",
                   include_plotlyjs='cdn')#make a folder for each location
    


# In[41]:


#iterate through location_ids to grab parameters for each location
# def main_flow(loc):
#     loc_data = get_data(loc)
#     for param in loc_data["parameters"]:
#         param_name = parameter_dict[param["parameterId"]]
#         unit_name = unit_dict[param["unitId"]]
#         loc_df = pd.DataFrame(param["readings"])
#         #one_plotly(loc_df, f"{loc} {param_name}", unit_name)
#         print(loc_df.head())


# ## 5. Build Graph and .write_html

# In[ ]:


#.write_html into a folder for each location, and upload the folder in relevant GitHub page


# In[43]:


# def download_plotly_html(loc):
#     loc_data = get_data(loc)
#     for param in loc_data["parameters"]:
#         param_name = parameter_dict[param["parameterId"]]
#         unit_name = unit_dict[param["unitId"]]
#         loc_df = pd.DataFrame(param["readings"])
#         plotly_graph(loc_df, loc, param_name, unit_name)


# In[45]:


#one_plotly("Radcliffe Outflow AquaTroll")


# # Main Function Sequence

# In[48]:


#1. Ensure access token is updated and active
access_token = update_access_token()

#2. Set up dictionary with all available locations
#locations_dict = get_locations()

#3. Loop through locations_dict and grab data for each location
# Currently, this loops through each location and writes the html to a folder
# one html file per parameter and per location
# for loc in location_ids:
#     download_plotly_html(loc)


# In[50]:


#main_flow("SE Creek AquaTroll")


# ## View Active Locations

# In[53]:


#print(get_locations())


# In[ ]:





# ## SECTION 2

# In[57]:





# In[85]:


# The local_git_token may need manual updates in GitHub
git_headers = {
    "Authorization" : f"token {LOCAL_GIT_TOKEN}",
    "Accept": "application/vnd.github+json",
    "User-Agent": "data_update"
}


# In[61]:


#git_body_params = {
#    "message" : "Updating the plots",
#    "content" : content_base64
#}


# In[63]:


# Path Parameter constants
OWNER = "WC-CES-Watershed-Innovation-Lab"
REPO  = "WaterQualityData"

# PATH needs to be updated for each new graph
#PATH  = f"docs/{loc}/{wq_param}"


# In[65]:


# Checks if file exists and returns necessary information
def file_exists(url):
    global git_headers
    
    meta = requests.get(url, headers=git_headers)
    if meta.status_code == 200:
        return meta.json()["sha"]
    else:
        return None


# In[67]:


# Make the request

def git_api_call(url, content):
    global git_headers

    sha = file_exists(url)
    
    git_body_params = {
    "message" : "Updating the plots",
    "content" : content,
    }

    if sha is not None:
        git_body_params["sha"] = sha
    
    
    response = requests.put(url, headers=git_headers, json=git_body_params)
    response.raise_for_status()
    print(response.json())


# In[69]:


def convert_dates(time_array):
    new_dates = []
    for val in time_array:
        local_datetime = datetime.fromtimestamp(val)
        new_dates.append(local_datetime)
    return new_dates

# In[]:
    

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


# # Below print out strings necessary for html page setup


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

for loc in location_ids:
    dfs_to_convert = dfs_from_csvs(loc) # NoneType if no csv exists
    if dfs_to_convert:
        for df in dfs_to_convert:
            param_name = df.iloc[0,3]
            unit_name = df.iloc[0,4]
            plotly_bytes(df, loc, param_name, unit_name)
# In[ ]:


#########################
#Run this to print out the html access lines for the index.html file on github

# def print_links(loc):
#     loc_data = get_data(loc)
#     for param in loc_data["parameters"]:
#         param_name = parameter_dict[param["parameterId"]]
#         encoded_spaces = param_name.replace(' ', '%20')
#         print(f"<li><a href=\"Millington%20AquaTroll/{encoded_spaces}.html\">{param_name}</a></li>")
# #print_links("Morgan Creek AquaTroll")


# In[ ]:


# def print_graph_paths(loc):
#     loc_data = get_data(loc)
#     for param in loc_data["parameters"]:
#         param_name = parameter_dict[param["parameterId"]]
#         encoded_spaces = param_name.replace(' ', '%20')
#         encoded_loc = loc.replace(' ', '%20')
#         print(f"<option value=\"{encoded_loc}/{encoded_spaces}.html\">{param_name}</option>")
# #print_graph_paths("Radcliffe Outflow AquaTroll")


# In[ ]:


# def print_graph_paths(loc):
#     loc_data = get_data(loc)
#     for param in loc_data["parameters"]:
#         param_name = parameter_dict[param["parameterId"]]
#         encoded_spaces = param_name.replace(' ', '%20')
#         encoded_loc = loc.replace(' ', '%20')
#         print(f"[\"{param_name}\", \"{encoded_loc}/{encoded_spaces}.html\"],")
# #print_graph_paths("SE Creek AquaTroll")


# # Define run suquence and run_ for each location (updates the plotly graph & data sent to github)

# In[87]:


# def run_all(d):
#     for key,value in dict.items():
#         for unit_key, df_value in value.items():
#             param_name = df_value.iloc[0,2]
#             unit_name = df_value.iloc[0,3]
#             plotly_bytes(df_value, key, param_name, unit_name)


# In[89]:


#run_all(loc_data_dict)


# In[ ]:


#print(loc_data_dict)


# In[ ]:


# for key, value in loc_data_dict.items():
#     for unit_key, df_value in value.items():
#         print(unit_key)
#     print()


# In[ ]:


#run_all(loc_data_dict)


# In[ ]:


# def run_(loc):
#     loc_data = get_data(loc)
#     for param in loc_data["parameters"]:
#         param_name = parameter_dict[param["parameterId"]]
#         unit_name = unit_dict[param["unitId"]]
#         loc_df = pd.DataFrame(param["readings"])
#         plotly_bytes(loc_df, loc, param_name, unit_name)


# In[ ]:


#run_("Millington AquaTroll")


# In[ ]:


# for loc in location_ids:
#     run_(loc)


# In[ ]:


# url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/docs/graphs/


# In[ ]:


# # Make the request
# response = requests.put(url, headers=headers, json=git_body_params)
# response.raise_for_status()
# print(response.json())
print("--- %s seconds ---" % (time.time() - start_time))

