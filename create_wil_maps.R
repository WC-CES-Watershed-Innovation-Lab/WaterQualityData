
library(tidyverse)
library(data.table)
library(leaflet)
library(htmlwidgets)
library(plotly)

# Stadia.AlidadeSatellite

weather_sites <- fread("weather_sites.csv")

weather <- leaflet(weather_sites) %>%
  # addProviderTiles(providers$Stadia.AlidadeSatellite) %>%
  addTiles() %>%
  setView(lng = -76.03779658645445, lat = 39.221877506563985, zoom = 13) %>%
  addMarkers(lng = ~long, lat = ~lat, label = ~site_name)

outpath <- "C:/Users/scheng2/OneDrive - Washington College/Documents/Repos/WaterQualityData"

outpath_weather <- file.path(outpath, "wil_weather_map.html")

htmlwidgets::saveWidget(as_widget(weather), outpath_weather)


# Now create water quality maps
wq_sites <- fread("wq_sites.csv")

wq <- leaflet(wq_sites) %>%
  # addProviderTiles(providers$Stadia.AlidadeSatellite) %>%
  addTiles() %>%
  # setView(lng = -76.03779658645445, lat = 39.221877506563985, zoom = 13) %>%
  addMarkers(lng = ~long, lat = ~lat, label = ~site_name)

outpath <- "C:/Users/scheng2/OneDrive - Washington College/Documents/Repos/WaterQualityData"

outpath_wq <- file.path(outpath, "wil_wq_map.html")

htmlwidgets::saveWidget(as_widget(wq), outpath_wq)




