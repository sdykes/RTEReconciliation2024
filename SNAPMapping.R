library(tidyverse)
#
# Below is code to read in and tidy-up/complete the Snap Orchard Master Data 
#

MasterData <- function(SNAPfile,Harvista,Ethrel) {

  SurplusPlantingID <- c(35,38,40,42,135,136,139,140,141,142,143,203,204,205,206,619,620)

  RiversideOrchard <- tibble(PlantingID = c(621,622,623,624,625,626,663,669),
                             RS.ManagementArea = c("B1","B1","C1","C1","D2","D2","F","A"),
                             RS.Subdivision = c("B","B","C","C","D","D","F","A"))

  RootStockCorrections <- tibble(PlantingID = c(444,446,448,453,455,490,567,568,569,570,572,
                                                573,574,seq(575,585,1),593,seq(610,613,1),676),
                                 COR.RootStock = c("M793","M793","M9","M9","M793","M9",
                                                   rep("M9",23),"CG202"))



  SnapOrchardMapping <- read_csv(SNAPfile, show_col_types = F) |>
    filter(!str_detect(PlantingCode, "\\w+\\sSEC"),
           !str_detect(PlantingCode, "^QUALITY$"),
           !str_detect(PlantingCode, "^RPC$")) |>
    mutate(Latitude = case_when(PIN == "R2936" ~ -38.6483459,
                                PIN == "R2937" ~ -38.6483459,
                                PIN == "R2943" ~ -38.6283100,
                                PIN == "R2947" ~ -38.6375400,
                                PIN == "R2920" ~ -39.3502572,
                                PIN == "R2958" ~ -38.6200000,
                                PIN == "R2967" ~ -38.7716600,
                                PIN == "R1875" ~ -39.5857320,
                                PIN == "R2968" ~ -38.4807170,
                                PIN == "R2969" ~ -39.9637410,
                                PIN == "R2897" ~ -39.8718220,
                                PIN == "R1651" ~ -39.5572588,
                                PIN == "R2695" ~ -38.6623750,
                                PIN == "R1172" ~ -39.5546050,
                                PIN == "R1351" ~ -39.5989150,
                                PIN == "R1173" ~ -39.5657930,
                                PIN == "R1096" ~ -39.5677180,
                                PIN == "R2933" ~ -38.6398641,
                                PIN == "R2952" ~ -38.705562,
                                TRUE ~ Latitude),
          Longitude = case_when(PIN == "R2936" ~ 177.9291534,
                                PIN == "R2937" ~ 177.933231,
                                PIN == "R2943" ~ 177.8808400,
                                PIN == "R2947" ~ 177.8350400,
                                PIN == "R2920" ~ 176.9093641,
                                PIN == "R2958" ~ 177.8800000,
                                PIN == "R2967" ~ 177.9044540,
                                PIN == "R1875" ~ 176.8041190,
                                PIN == "R2968" ~ 177.8734330,
                                PIN == "R2969" ~ 176.4006670,
                                PIN == "R2897" ~ 176.4011990,
                                PIN == "R1651" ~ 176.8833384,
                                PIN == "R2695" ~ 177.9301510,
                                PIN == "R1172" ~ 176.8812450,
                                PIN == "R1351" ~ 176.9288160,
                                PIN == "R1173" ~ 176.8107970,
                                PIN == "R1096" ~ 176.7998120,
                                PIN == "R2933" ~ 177.9348355,
                                PIN == "R2952" ~ 177.841851,
                                TRUE ~ Longitude),
          QtyTrees = case_when(PlantingID == 621 ~ 149,
                                PlantingID == 622 ~ 750,
                                PlantingID == 623 ~ 1442,
                                PlantingID == 624 ~ 1559,
                                PlantingID == 625 ~ 530,
                                PlantingID == 626 ~ 626,
                                TRUE ~ QtyTrees),
          Hectares = case_when(PlantingID == 621 ~ .148,
                               PlantingID == 622 ~ .560,
                               PlantingID == 623 ~ .87,
                               PlantingID == 624 ~ .94,
                               PlantingID == 625 ~ .596,
                               PlantingID == 626 ~ .600,
                               TRUE ~ Hectares),
          `Grow System` = case_when(PIN == "R1021" ~ "Top Grafted",
                                    PIN == "R1095" ~ "Spindle",
                                    PIN == "R1096" ~ "2D",
                                    PIN == "R1172" ~ "Top Grafted",
                                    PIN == "R1173" ~ "2D",
                                    PIN == "R1222" ~ "Top Grafted",
                                    PIN == "R1262" ~ "2D",
                                    PIN == "R1351" ~ "Top Grafted",
                                    PIN == "R1352" ~ "Top Grafted",
                                    PIN == "R1369" ~ "Spindle",
                                    PIN == "R1533" ~ "2D",
                                    PIN == "R1611" ~ "Top Grafted",
                                    PIN == "R1626" ~ "Top Grafted",
                                    PIN == "R1632" ~ "Top Grafted",
                                    PIN == "R1651" ~ "2D",
                                    PIN == "R1751" ~ "Top Grafted",
                                    PIN == "R1752" ~ "Top Grafted",
                                    PIN == "R1811" ~ "2D",
                                    PIN == "R1856" ~ "Spindle",
                                    PIN == "R1862" ~ "Spindle",
                                    PIN == "R1875" ~ "2D",
                                    PIN == "R1899" ~ "Top Grafted",
                                    PIN == "R2124" ~ "Top Grafted",
                                    PIN == "R2695" ~ "2D",
                                    PIN == "R2710" ~ "Top Grafted",
                                    PIN == "R2732" ~ "Spindle",
                                    PIN == "R2779" ~ "2D",
                                    PIN == "R2786" ~ "Spindle",
                                    PIN == "R2841" ~ "Spindle",
                                    PIN == "R2895" ~ "Top Grafted",
                                    PIN == "R2897" ~ "2D",
                                    PIN == "R2915" ~ "2D",
                                    PIN == "R2920" ~ "2D",
                                    PIN == "R2929" ~ "Spindle",
                                    PIN == "R2933" ~ "2D",
                                    PIN == "R2934" ~ "2D",
                                    PIN == "R2936" ~ "2D",
                                    PIN == "R2937" ~ "2D",
                                    PIN == "R2943" ~ "2D",
                                    PIN == "R2947" ~ "2D",
                                    PIN == "R2949" ~ "2D",
                                    PIN == "R2954" ~ "2D",
                                    PIN == "R2967" ~ "2D",
                                    PIN == "R2968" ~ "2D",
                                    PIN == "R2969" ~ "2D",
                                    TRUE ~ `Grow System`),
# Corrections to Sunvalley         
          ManagementArea = case_when(PlantingID == 456 ~ "EAST",
                                     PlantingID == 493 ~ "NORTH",
                                     PlantingID == 459 ~ "WEST",
                                     TRUE ~ ManagementArea),
# corrections to Swamp Road
          Subdivision = case_when(PlantingID == 496 ~ "A",
                                  PlantingID == 497 ~ "B",
                                  PlantingID == 498 ~ "C",
                                  PlantingID == 499 ~ "D",
                                  PlantingID == 500 ~ "E",
                                  PlantingID == 501 ~ "F",
                                  TRUE ~ Subdivision),
# corrections to Kararua
          Subdivision = case_when(PlantingID == 550 ~ "C",
                                  PlantingID == 551 ~ "C",
                                  TRUE ~ Subdivision),
          ManagementArea = case_when(PlantingID == 552 ~ "ROC10",
                                     TRUE ~ ManagementArea)) |>
# 
# remove all of the obsolete (inactive blocks)  
#
   filter(!(PlantingID %in% SurplusPlantingID)) |>
#
# Repair Richard Griffith's Riverside Orchard Blocks  
#
   left_join(RiversideOrchard, by = "PlantingID") |>
   mutate(ManagementArea = coalesce(ManagementArea, RS.ManagementArea),
          Subdivision = coalesce(Subdivision, RS.Subdivision),
# remove the full-stops from the rootstocks
          RootStock = str_remove_all(RootStock,"[.]"),
# convert everything to upper case
          RootStock = str_to_upper(RootStock)) |>
   left_join(RootStockCorrections, by = "PlantingID") |>
   mutate(RootStock = coalesce(RootStock, COR.RootStock)) |>
   select(-c(RS.ManagementArea,RS.Subdivision,COR.RootStock))
#
# This reads the Block data in from ABC to compare to SNAP table
#
  con <- DBI::dbConnect(odbc::odbc(),    
                        Driver = "ODBC Driver 18 for SQL Server", 
                        Server = "abcrepldb.database.windows.net",  
                        Database = "ABCPackerRepl",   
                        UID = "abcadmin",   
                        PWD = "Trauts2018!",
                        Port = 1433
  )

  OrchardRegisterPS <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2024/OrchardRegister.sql")) |>
    ungroup() |>
    group_by(FarmName, PIN, Subdivision) |>
    summarise(Owner = first(Owner),
              ManagerName = first(ManagerName),
              .groups = "drop")

  DBI::dbDisconnect(con)

##################################################################################
#                 Summarise by PS instead of Management Area                     #
##################################################################################

  SnapOrchardMappingPS <- SnapOrchardMapping |>
    group_by(PIN,Orchard,Subdivision) |>
    summarise(`Grow System` = first(`Grow System`),
              RootStock = first(RootStock),
              YearPlanted = first(YearPlanted),
              Latitude = first(Latitude),
              Longitude = first(Longitude),
              RowSpacing = first(RowSpacing),
              TreeSpacing = first(TreeSpacing),
              QtyTrees = sum(QtyTrees),
              QtyRows = sum(QtyRows),
              Hectares = sum(Hectares),
              .groups = "drop") 

  MergedPSMD <- OrchardRegisterPS |>
    inner_join(SnapOrchardMappingPS, by = c("PIN", "Subdivision"))

  MasterDataByPS <- MergedPSMD |>
    left_join({{Ethrel}}, by = c("PIN","Subdivision")) |>
    left_join({{Harvista}}, by = c("PIN","Subdivision")) |>
    mutate(across(.cols = c(Harvista, Ethrel), ~replace_na(.,0))) |>
    mutate(YearPlanted = case_when(PIN == "R1352" ~ 2021,
                                   PIN == "R2937" ~ 2022,
                                   TRUE ~ YearPlanted),
           OrchardAge = year(Sys.Date())-YearPlanted,
           RootStock = if_else(RootStock == "M116", "M9/M116", RootStock)) |>
    rename(FarmCode = PIN,
           ProductionSite = Subdivision) 
  
  return(MasterDataByPS)
}

MasterDataByPS <- MasterData("data/SnapOrchardMapping.csv",HarvistaSummaryPS,EthrelSummaryPS)









