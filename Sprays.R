library(tidyverse)

Harvista <- function(Harvistafile) {

  HarvistaDiary <- read_csv(Harvistafile,show_col_types = F) |>
    filter(Chemical == "Harvista 1.3") |>
    mutate(Date = as.Date(Date, "%d/%m/%Y")) |>
    rename(Subdivision = ProductionSite)
  
  HarvistaSummary <- HarvistaDiary |>
    group_by(PIN, Subdivision, ManagementArea) |>
    summarise(NoOfApplications = n(),
              .groups = "drop") |>
    mutate(Harvista = 1) |>
    select(-c(NoOfApplications))

  HarvistaSummaryPS <- HarvistaSummary |>
    group_by(PIN,Subdivision) |>
    summarise(Harvista = first(Harvista),
              .groups = "drop")
  
  return(HarvistaSummaryPS)
}


Ethrel <- function(Ethrelfile) {
  
  EthrelDiary <- read_csv(Ethrelfile,show_col_types = F) |>
    filter(Chemical == "Ethin") |>
    rename(Subdivision = ProductionSite) |>
    mutate(Date = as.Date(Date, "%d/%m/%Y"),
           ApplicationRate = case_when(ChemicalUnit == "ml/100 litres" ~ ChemicalRate*0.01*WaterRate,
                                       ChemicalUnit == "l/100 litres" ~ ChemicalRate*10*WaterRate,
                                       ChemicalUnit == "ml/Ha" ~ ChemicalRate),
          Rate = if_else(ApplicationRate > 330, "High Rate", "Low Rate")) |>
    filter(Date > as.Date("2024-01-01") & Date < as.Date("2024-03-01"))

  EthrelSummary <- EthrelDiary |>
    filter(Rate == "High Rate") |>
    group_by(PIN, Subdivision, ManagementArea) |>
    summarise(NoOfApplications = n(),
              .groups = "drop") |>
    mutate(Ethrel = 1) |>
    select(-c(NoOfApplications))

  EthrelSummaryPS <- EthrelSummary |>
    group_by(PIN,Subdivision) |>
    summarise(Ethrel = first(Ethrel),
              .groups = "drop")
  
  return(EthrelSummaryPS)
}

HarvistaSummaryPS <- Harvista("data/SprayDiary.csv")

EthrelSummaryPS <- Ethrel("data/SprayDiary.csv")


