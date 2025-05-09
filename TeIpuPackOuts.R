library(tidyverse)

source("../CommonFunctions/getSQL.r")

con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPackerRepl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

GBDTeIpu <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLFiles2024/GraderBatchTeIpu.sql")) |>
  filter(Season == 2024,
         `Packing site` == "Te Ipu Packhouse (RO)",
         !PresizeInputFlag) |>
  mutate(StorageDays = as.integer(PackDate - HarvestDate),
         PackOut = 1-RejectKgs/InputKgs) |>
  select(-c(PresizeInputFlag))

DBI::dbDisconnect(con)


  








