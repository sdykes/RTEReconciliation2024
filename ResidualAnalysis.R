library(tidyverse)
#
# The residual adjustment looks at the net residual (for each grower) based on a linear model with 
# packout and StorageDays (The principal predictor variable).  The idea is that the net residual is 
# a measure of the packout perfomance compared to the conditional mean packout.  This is calculated for 
# Te Ipu and SF packouts for each grower and the SF net residual is then adjusted to the same level as the 
# Te Ipu
#

# Te Ipu Actual Packout

ResidualAdjustment <- function(lasso_final_fit,TrainingTable,TestTable) {

  ModeledTrainingData <- lasso_final_fit |>
    augment(new_data = TrainingTable) |>
    filter(MaturityCode %in% c("A", "B", "C")) |>
    mutate(`Storage type` = if_else(`Storage type` == "Normal Air", 
                                    "Smartfreshed",
                                    `Storage type`))

  ResidualModelTI <- lm(PackOut~StorageDays, data = ModeledTrainingData) 

  ResidualsTI <- tibble(Residuals = ResidualModelTI$residuals)

  MTDWithResiduals <- ModeledTrainingData |>
    bind_cols(ResidualsTI)

  WARByGrower <- MTDWithResiduals |>
    group_by(Owner) |>
    summarise(WAResidualsTI = weighted.mean(Residuals,InputKgs, na.rm=T)) 

# Take Normalised Packout from SF Method 1 - take previous regression and calculate residual using normalised PO

  ModeledTrainingDataSF <- lasso_final_fit |>
    augment(new_data = TestTable |> rename(`Storage type` = StorageType)) |>
    filter(MaturityCode %in% c("A", "B", "C")) |>
    mutate(`Storage type` = if_else(`Storage type` == "Normal Air", 
                                    "Smartfreshed",
                                    `Storage type`)) 
  
  ResidualModelSF <- lm(.pred~StorageDays, data = ModeledTrainingDataSF) 

#summary(ResidualModelSF)

  ResidualModelSFPred <- tibble(.pred = predict(ResidualModelSF, newdata = ModeledTrainingDataSF)) |>
    rename(ModeledPackOut = .pred)

  ResidualsSF <- tibble(Residuals = ResidualModelSF$residuals)
  
  SFModWithResid <- ModeledTrainingDataSF |>
    bind_cols(ResidualModelSFPred) |>
    bind_cols(ResidualsSF)

  WARByGrowerSFL <- SFModWithResid |>
    group_by(Owner) |>
    summarise(WAResidualsSFL = weighted.mean(Residuals,InputKgs, na.rm=T)) 

# Joining the tables together

  MasterResidual <- WARByGrower |>
    left_join(WARByGrowerSFL, by = "Owner") |>
    mutate(AdjustmentL = WAResidualsTI-WAResidualsSFL) 

#write_csv(MasterResidual, "ResidualAndAdjustmentSummary.csv")

  return(MasterResidual)
}
#
# Making the adjustment to the sunfruit runs
#

ResidualAdjustment <- ResidualAdjustment(lasso_final_fit,TrainingTable,TestTable)


SFResidAdjust <- SFPackRunWithNormalisedPO |>
  mutate(NormExportKgs = .pred*InputKgs,
         ExportKgs = PackOut*InputKgs) |>
  group_by(Owner) |>
  summarise(InputKgs = sum(InputKgs),
            ExportKgs = sum(ExportKgs),
            NormExportKgs = sum(NormExportKgs)) |>
  mutate(PackOut = ExportKgs/InputKgs,
         NormPackOut = NormExportKgs/InputKgs) |>
  left_join(ResidualAdjustment |> 
              select(c(Owner, AdjustmentL)),
            by = "Owner") |>
  mutate(ResidAdjustedPred = NormPackOut + AdjustmentL,
         RAPFactor = ResidAdjustedPred/NormPackOut)

#
#  Expanding to all batches using the RAP factor
#
#
#  First convert all SF pack runs to GraderBatches in ABC
#

SFResidAdjustGB <- function(SFPackRunWithNormalisedPO,SFResidAdjust,MappedInputBins) {

  con <- DBI::dbConnect(odbc::odbc(),    
                        Driver = "ODBC Driver 18 for SQL Server", 
                        Server = "abcrepldb.database.windows.net",  
                        Database = "ABCPackerRepl",   
                        UID = "abcadmin",   
                        PWD = "Trauts2018!",
                        Port = 1433
  )

  GBDSF <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLFiles2024/GraderBatchTeIpu.sql")) |>
    filter(Season == 2024,
           `Packing site` == "Sunfruit Limited",
           !PresizeInputFlag) |>
    mutate(StorageDays = as.integer(PackDate - HarvestDate),
           PackOut = 1-RejectKgs/InputKgs) |>
    select(-c(PresizeInputFlag))

  DBI::dbDisconnect(con)

  SFGraderBatchSummary <- SFPackRunWithNormalisedPO |>
    left_join(MappedInputBins |>
                select(c(Run, GraderBatchID)),
              by = "Run") |>
    mutate(SFNormExportKgs = .pred*InputKgs) |>
    group_by(GraderBatchID) |>
    summarise(Bins = sum(Bins),
              SFInputKgs = sum(InputKgs),
              SFNormExportKgs = sum(SFNormExportKgs)) |>
    mutate(PackOut = SFNormExportKgs/SFInputKgs) |>
    left_join(GBDSF |>
                select(c(GraderBatchID, Owner, Orchard, RPIN, `Production site`)),
              by = "GraderBatchID") |>
    left_join(SFResidAdjust |>
                select(c(Owner, RAPFactor)),
              by = "Owner") |>
    mutate(RAPAdjustedPO = PackOut*RAPFactor)

  return(SFGraderBatchSummary)
}

SFResidAdjustGB <- SFResidAdjustGB(SFPackRunWithNormalisedPO,SFResidAdjust,MappedInputBins)




  
