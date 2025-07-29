# load libraries
library(here)
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(data.table)
library(hms)
library(lubridate)
library(tidyr)

# set directory values
rawdata.folder = "data/raw-data"
subprocessed.data.folder = "data/subprocessed-data"
processed.data.folder = "data/processed-data"

# define custom operator
`%notin%` <- Negate(`%in%`)

# ---------- 
# Import Metadata
# ----------

metadata <- metadata <- read.csv(here("respirometry-metadata.csv"))

# ----------
# Define functions
# ----------

# define function to process raw file
process.file <- function(rawdata.filename, metadata, subprocessed.data.folder) {
  
  data.all.chs <- read.table(file = here(rawdata.folder, rawdata.filename),
                             sep = '\t', # tab-delimited tables
                             fileEncoding = "UTF-16LE", # enables function to read unicode table
                             header = TRUE, # makes the row above the data into the column names
                             skip = 22 ) # ignores rows in the file header
  
  data.all.chs <- data.all.chs %>% 
    rename(date = Date..DD.MM.YYYY., Time = Time..HH.MM.SS., Temperature = Temperature.CH.1,
           Salinity = Salinity.CH.1, Pressure = Pressure.CH.1) %>%
    select(date, Time, 
           Temperature, Pressure, Salinity, 
           Oxygen.CH.1, Phase.CH.1, Amplitude.CH.1, 
           Oxygen.CH.2, Phase.CH.2, Amplitude.CH.2, 
           Oxygen.CH.3, Phase.CH.3, Amplitude.CH.3, 
           Oxygen.CH.4, Phase.CH.4, Amplitude.CH.4) %>%
    mutate(date = mdy(date), # fix column types of date and time columns
           Time = Time %>% parse_time() %>% as_hms())
  
  # now get the metadata for the file as object 'working.metadata'
  working.metadata <- metadata %>% filter(filename == str_remove(rawdata.filename, ".csv"))
  
  for (row in 1:nrow(data.all.chs)) {
    if (data.all.chs[row, 'Time'] < working.metadata$m1start[1]) {
      data.all.chs[row, 'Timephase'] <- "Acclimation"
    } else if (data.all.chs[row, 'Time'] > working.metadata$m1start[1] & data.all.chs[row, 'Time'] < working.metadata$m1end[1]) {
      data.all.chs[row, 'Timephase'] <- "M1"
    } else if (data.all.chs[row, 'Time'] > working.metadata$m1end[1] & data.all.chs[row, 'Time'] < working.metadata$m2start[1]) {
      data.all.chs[row, 'Timephase'] <- "Reoxygenation"
    } else if (data.all.chs[row, 'Time'] > working.metadata$m2start[1] & data.all.chs[row, 'Time'] < working.metadata$m2end[1]) {
      data.all.chs[row, 'Timephase'] <- "M2"
    } else if (data.all.chs[row, 'Time'] > working.metadata$m2end[1] & data.all.chs[row, 'Time'] < working.metadata$m3start[1]) {
      data.all.chs[row, 'Timephase'] <- "Heating"
    } else if (data.all.chs[row, 'Time'] > working.metadata$m3start[1] & data.all.chs[row, 'Time'] < working.metadata$m3end[1]) {
      data.all.chs[row, 'Timephase'] <- "M3"
    } else if (data.all.chs[row, 'Time'] > working.metadata$m3end[1]) {
      data.all.chs[row, 'Timephase'] <- "End"
    } 
  }
  
  data.all.chs <- data.all.chs %>% relocate(Timephase, .after = Salinity)
  
  write_csv(data.all.chs, file = here(subprocessed.data.folder, rawdata.filename))
  
}

# define function to split processed file into individual channels
split_csv_by_channel <- function(input.file, output.dir, metadata.obj = metadata) {
  # Read the original CSV
  original.data <- read.csv(input.file)
  
  # Get the base filename (without extension)
  base.filename <- tools::file_path_sans_ext(basename(input.file))
  
  # get the metadata and lane range for the file
  working.metadata <- metadata.obj %>% filter(filename == base.filename)
  lane.range <- range(working.metadata$lane)
  
  # Reshape the data to a long format for easier processing
  long.data <- original.data %>%
    pivot_longer(
      cols = starts_with("Oxygen.CH") | starts_with("Phase.CH") | starts_with("Amplitude.CH"),
      names_to = c(".value", "channel"),
      names_pattern = "(Oxygen|Phase|Amplitude)\\.CH\\.(\\d+)"
    ) %>%
    mutate(channel = as.integer(channel), # Convert channel to numeric for sorting
           filename = base.filename)      # Add 'filename' column
  
  if (lane.range[1] == 5) {
    long.data <- long.data %>% mutate(channel = channel + 4)
  }
  
  # Split the data by channel and save each as a separate CSV
  for (ch in unique(long.data$channel)) {
    
    channel.metadata <- working.metadata %>% filter(lane == ch)
    
    channel.data <- long.data %>%
      filter(channel == ch) %>%
      select(
        channel,
        filename,
        everything()
      ) %>% 
      mutate(
        pop = channel.metadata$pop, 
        emb.trt = channel.metadata$emb.trt, 
        str.trt = channel.metadata$str.trt, 
        .after = date
      )
    
    # Save to CSV
    write_csv(channel.data,
              file = file.path(here(output.dir), paste0(base.filename, "_channel_", ch, ".csv"))
    )
  }
}

# ---------
# Batch process files
# ---------

# list files that need processing
rawdatafiles.list <- list.files(here(rawdata.folder))
already.processed.rawdata <- list.files(here(subprocessed.data.folder))

# if a file doesn't have a corresponding row in the metadata table, we won't be able to process it. 
# now let's select only the files in raw-data/ for which we have rows in the metadata table AND that haven't been processed already!
rawdatafiles.to.process <- rawdatafiles.list[rawdatafiles.list %notin% already.processed.rawdata] 
rawdatafiles.to.process <- rawdatafiles.to.process[rawdatafiles.to.process %>% str_remove(".csv") %in% metadata$filename]

# process the files
if (length(rawdatafiles.to.process) == 0) {
  print("Error: there is no raw data available to process. The raw-data folder does not contain any compatible files, or all of the raw data has already been processed.")
} else {
  
  for (i in 1:length(rawdatafiles.to.process)) {
    
    rawdata.filename <- rawdatafiles.to.process[i]
    
    print(paste("processing file: ", rawdata.filename))
    
    process.file(rawdata.filename, metadata, subprocessed.data.folder)
    
    split_csv_by_channel(here(subprocessed.data.folder, rawdata.filename), processed.data.folder)
  }
  
  rm(i, rawdata.filename, rawdatafiles.to.process)
  
  print("Done.")
  
  print("Data files processed:")
  updated.processed.rawdata <- list.files(here(subprocessed.data.folder))
  newlyprocessed.rawdata <- updated.processed.rawdata[updated.processed.rawdata %notin% already.processed.rawdata]
  print(newlyprocessed.rawdata)
  
  # if any files didn't have a corresponding row in the metadata table, the code would be unable to process them.
  # they'll be listed here
  print("Data files in raw-data/ that could not be processed:")
  rawdata.notprocessed <- rawdatafiles.list[rawdatafiles.list %notin% updated.processed.rawdata]
  print(rawdata.notprocessed)
  
  rm(updated.processed.rawdata, newlyprocessed.rawdata, already.processed.rawdata, rawdata.notprocessed, rawdatafiles.list)
}