library(tidyverse)

TrainingTable <- function(GBDTeIpu,MasterData) {
  
  TrainingTable <- {{GBDTeIpu}} |>
    rename(FarmCode = RPIN,
           FarmName = Orchard,
           ProductionSite = `Production site`) |>
    select(-c(Owner)) |>
    inner_join({{MasterData}}, by = c("FarmCode","FarmName","ProductionSite")) |>
    select(-c(`Packing site`,FarmName,RowSpacing,TreeSpacing,YearPlanted,QtyTrees,
              QtyRows,RejectKgs, ManagerName)) |>
    mutate(HarvestDays = yday(HarvestDate)) |>
    filter(!is.na(PackOut))
  
  #write_csv(FinalTrainingTable, "FinalTeIpuTable.csv")
  
  return(TrainingTable)

}

TrainingTable <- TrainingTable(GBDTeIpu,MasterDataByPS)

FinalTrainingTable <- TrainingTable |>
  select(-c(Season, Orchard, FarmCode, ProductionSite, GraderBatchID, GraderBatchNo, 
            Owner, PackDate, HarvestDate, FieldBinsTipped, BatchStatus)) |>
  filter(!is.na(PackOut))
