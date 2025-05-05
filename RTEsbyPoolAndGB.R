library(tidyverse)


RTEsByPool <- function() {

#
# Load the SQL parsing function
#

  source("../CommonFunctions/getSQL.r")

  con <- DBI::dbConnect(odbc::odbc(),    
                        Driver = "ODBC Driver 18 for SQL Server", 
                        Server = "abcrepldb.database.windows.net",  
                        Database = "ABCPackerRepl",   
                        UID = "abcadmin",   
                        PWD = "Trauts2018!",
                        Port = 1433
  )

  BinPresizeOutput <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles/Bin_Presize_Output.sql"))

  OutputFromFieldBins <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles/ma_Output_From_Field_Bins.sql"))  

  GraderBatchRepackProduction <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles/ma_Grader_Batch_Repack_Production.sql"))

  PoolDefintion <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles/PoolDefinition.sql"))

  GBD <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles/GraderBatchTeIpu.sql"))

  DBI::dbDisconnect(con)

  ExportBins <- OutputFromFieldBins |>
    filter(SeasonID == 2011) |>
    left_join(PoolDefintion |>
                select(-c(ProductDesc)), 
              by = "ProductID") |>
    group_by(GraderBatchID, PoolDesc) |>
    summarise(RTEs = sum(RTEs, na.rm=T),
              Kgs = sum(Kgs, na.rm=T),
              .groups = "drop") |>
    mutate(Source = "ExportBins") 

  RepackBins <- GraderBatchRepackProduction |>
    filter(SeasonID == 2011) |>
    left_join(PoolDefintion |>
                select(-c(ProductDesc)), 
              by = "ProductID") |>
    group_by(GraderBatchID, PoolDesc) |>
    summarise(RTEs = sum(RTEs, na.rm=T),
              Kgs = sum(Kgs, na.rm=T),
              .groups = "drop") |>
    mutate(Source = "RepackBins") 

  PresizeBins <- BinPresizeOutput |>
    filter(SeasonID == 2011) |>
    left_join(PoolDefintion |>
                select(-c(ProductDesc)), 
              by = c("PresizeProductID" = "ProductID")) |>
    group_by(GraderBatchID, PoolDesc) |>
    summarise(RTEs = sum(RTEs, na.rm=T),
              Kgs = sum(Kgs, na.rm=T),
              .groups = "drop") |>
    mutate(Source = "PresizeBins") 

  RTEsByPool <- ExportBins |>
    bind_rows(RepackBins) |>
    bind_rows(PresizeBins) |>
    group_by(GraderBatchID, PoolDesc) |>
    summarise(RTEs = sum(RTEs),
              Kgs = sum(Kgs),
              .groups = "drop") |>
    mutate(PoolTemp = str_sub(PoolDesc, 15,-1),
           PoolTemp = as.character(PoolTemp),
           Pool = case_when(PoolTemp %in% c("0","145","160") ~ "Bulk",
                            TRUE ~ PoolTemp)) |>
    select(-c(PoolDesc, PoolTemp)) |>
    group_by(GraderBatchID, Pool) |>
    summarise(RTEs = sum(RTEs),
              .groups = "drop") |>
    pivot_wider(id_cols = GraderBatchID,
                names_from = Pool,
                values_from = RTEs,
                values_fill = 0)

# Work out the Kg for the bulk format so the Mean Apple mass can be used for RTE calculation

  BulkKgsByPool <- ExportBins |>
    bind_rows(RepackBins) |>
    bind_rows(PresizeBins) |>
    group_by(GraderBatchID, PoolDesc) |>
    summarise(RTEs = sum(RTEs),
              Kgs = sum(Kgs),
              .groups = "drop") |>
    mutate(PoolTemp = str_sub(PoolDesc, 15,-1),
           PoolTemp = as.character(PoolTemp),
           Pool = case_when(PoolTemp %in% c("0","145","160") ~ "Bulk",
                            TRUE ~ PoolTemp)) |>
    select(-c(PoolDesc, PoolTemp)) |>
    group_by(GraderBatchID, Pool) |>
    summarise(Kgs = sum(Kgs),
              .groups = "drop") |>
    pivot_wider(id_cols = GraderBatchID,
                names_from = Pool,
                values_from = Kgs,
                values_fill = 0) |>
    select(c(GraderBatchID, Bulk)) |>
    rename(BulkKgs = Bulk)

## Integrate the KGs part for the bulk back into the RTEsByPool df

  RTEsByPoolWithBulkKgs <- RTEsByPool |>
    left_join(BulkKgsByPool, by = "GraderBatchID")
  
  return(RTEsByPoolWithBulkKgs)
  
}

RTEsByPoolWithBulkKgs <- RTEsByPool()


#
# By owner
#

RTEsByOwner <- function(RTEsByPoolWithBulkKgs) {
  
  con <- DBI::dbConnect(odbc::odbc(),    
                        Driver = "ODBC Driver 18 for SQL Server", 
                        Server = "abcrepldb.database.windows.net",  
                        Database = "ABCPackerRepl",   
                        UID = "abcadmin",   
                        PWD = "Trauts2018!",
                        Port = 1433
  )
  
  GBD <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles/GraderBatchTeIpu.sql"))
  
  DBI::dbDisconnect(con)

  RTEsByOwner <- GBD |>
    filter(Season == "2024") |>
    select(c(GraderBatchID,Owner)) |>
    left_join(RTEsByPoolWithBulkKgs, by = "GraderBatchID") |>
    group_by(Owner) |>
    summarise(across(.cols = c(`53`:BulkKgs), ~sum(., na.rm=T)))

  return(RTEsByOwner)
}
  
RTEsByOwner <- RTEsByOwner(RTEsByPoolWithBulkKgs)

  





