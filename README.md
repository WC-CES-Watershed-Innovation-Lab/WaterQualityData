# WaterQualityData

This repository has two functions: code sharing and website compiling.

data_from_hydrovu_v2.py is the main codefile that forms the basis of this repo's functionality. With that program, we pull data from HydroVu and compile them into PyPlot graphs. Then, the program uploads PyPlot graph bit data to this GitHub repo.

The docs folder stores the graph data by location, with a code file for each water quality parameter. The index.html file structures the graphs into an easy-to-navigate web interface.

## Main Script -- data_from_hydrovu_v2.py

Code functionality: Makes calls to HydroVu to grab water quality data for each location using date-based pagination. The program can build new csvs [build_csv(loc, days)] or update existing csvs, based on the last recorded date in the csv [update_csv(loc)]. The final main process depends on the [plotly_bytes()] function, which takes a dataframe (converted from the existing csvs) and converts it into a plotly graph, writes the plotly graph to html, and sends the graph html to this repo's docs folder.


### Warnings

Duplicate points -- Because of date-based pagination logic, the end date/time for a given page matches the first date/time for the following page.

Secrets -- To run this code, you must enter a client id, client secret, and github token. When updating the data_from_hydrovu_v2.py file, remove these secrets so as to not comprimise this repo.


## Pages Site -- docs/ folder

The folder includes one folder per location as well as an index.html file. Each location folder includes an html file for each water quality parameter. The parameter html files include all the code necessary to display the pyplot graph of that site's historical data.
The index.html sets up the page. The page has buttons for each location, and each location has a drop-down menu to load the graph for the given parameter. 

Check out the page: https://wc-ces-watershed-innovation-lab.github.io/WaterQualityData/
