
library(tidyverse)
library(data.table)
library(leaflet)
library(htmlwidgets)
library(plotly)

# Stadia.AlidadeSatellite

weather_sites <- fread("data/monitoring_sites.csv") %>% filter(type == "weather")

weather <- leaflet(weather_sites) %>%
  # addProviderTiles(providers$Stadia.AlidadeSatellite) %>%
  addTiles() %>%
  setView(lng = -76.03779658645445, lat = 39.221877506563985, zoom = 13) %>%
  addMarkers(lng = ~long, lat = ~lat, label = ~site_name,
             labelOptions = labelOptions(textsize = "120%"))

outpath_weather <- file.path("wil_weather_map.html")

htmlwidgets::saveWidget(as_widget(weather), outpath_weather)


# Now create water quality maps
wq_sites <-  fread("data/monitoring_sites.csv") %>% filter(type == "water_quality")

wq <- leaflet(wq_sites) %>%
  # addProviderTiles(providers$Stadia.AlidadeSatellite) %>%
  addTiles() %>%
  # setView(lng = -76.03779658645445, lat = 39.221877506563985, zoom = 13) %>%
  addMarkers(lng = ~long, lat = ~lat, label = ~site_name,
             labelOptions = labelOptions(textsize = "120%"))

outpath_wq <- file.path("wil_wq_map.html")

htmlwidgets::saveWidget(as_widget(wq), outpath_wq)




