# --------- SET UP -----------
library(tidyverse)
library(data.table)
library(plotly)
library(ggeffects)
library(suncalc)
library(performance)
library(readnoaa)
library(htmlwidgets)

source_dir <- paste0("C:/Users/", Sys.getenv('USERNAME'), "/OneDrive - Washington College/Watershed Innovation Lab - Documents/General/Weather Stations/Data/cleaned")

# ------------ READ DATA ------------------
i <- list.files(source_dir, full.names = T)

# Read in data
rafc_dat <- fread(i[grepl("rafc", i)])
ces_dat <- fread(i[grepl("ces", i)])

rafc_dat$station <- "RAFC"
ces_dat$station <- "CES"

weather_dat <- rbind(rafc_dat, ces_dat, fill = T)

# Set key vars
key_vars <- c("timestamp_est", "timestamp_utc", "station")

# pivot longer weather
weather_dat <- pivot_longer(data = weather_dat, cols = names(weather_dat)[names(weather_dat) %in% key_vars ==F],
                            names_to = "parameter", values_to = "value")

# Read in weather display names
names_schema <- fread("C:/Users/scheng2/OneDrive - Washington College/Watershed Innovation Lab - Documents/General/Weather Stations/Data/weather_names_schema.csv")

# Create plotly figures
parameters <- unique(weather_dat$parameter)

weather_plot_dir <- "C:/Users/scheng2/OneDrive - Washington College/Documents/Repos/WaterQualityData/weather_plots"

for(n in 1:length(parameters)){
  temp_dat <- weather_dat %>%
    filter(parameter == parameters[n])
  
  display_name <- names_schema %>% 
    filter(parameter == parameters[n]) %>%
    pull(display_name)
  
  p <- ggplot(data = temp_dat)+
    geom_line(aes(x = timestamp_est, y = value, color = station))+
    labs(x = "Date/Time", color = "Location", y = display_name)+
    scale_x_datetime(date_breaks = "3 months", date_labels = ("%b %Y"))+
    scale_y_continuous(breaks = waiver(), n.breaks = 8)
    
  p2 <- ggplotly(p)
  
  plotname <- paste0(parameters[n], ".html")
  outpath <- file.path(weather_plot_dir, plotname)
  
  htmlwidgets::saveWidget(as_widget(p2), outpath)
}



