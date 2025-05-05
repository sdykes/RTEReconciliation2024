library(tidyverse)

SummaryByOwner <- function(TrainingTable,TestTable,RTEsByOwner,alpha) {

#
# Te Ipu PackOuts by Owner
#

  TIRaw <- TrainingTable |>
    mutate(TIExportKgs = PackOut*InputKgs) |>
    group_by(Owner) |>
    summarise(FieldBinsTipped = sum(FieldBinsTipped),
              TIInputKgs = sum(InputKgs),
              TIExportKgs = sum(TIExportKgs)) |>
    mutate(TIPackOut = TIExportKgs/TIInputKgs)
  
#
# Sunfruit Packouts by Owner
#

  SFRaw <- TestTable |>
    mutate(SFExportKgs = PackOut*InputKgs) |>
    group_by(Owner) |>
    summarise(Bins = sum(Bins),
              SFInputKgs = sum(InputKgs),
              SFExportKgs = sum(SFExportKgs)) |>
    mutate(SFPackOut = SFExportKgs/SFInputKgs)

#
# Normalised Packout and Residual adjustment
#

  SFResidAdjustSummary <- SFResidAdjust |>
    select(c(Owner,NormPackOut,AdjustmentL,ResidAdjustedPred))

  MasterSummaryByOwner <- TIRaw |>
    left_join(SFRaw, by = "Owner") |>
    relocate(Bins, .after = FieldBinsTipped) |>
    relocate(TIPackOut, .after = Bins) |>
    relocate(TIInputKgs, .after = TIPackOut) |>
    relocate(TIExportKgs, .after = TIInputKgs) |>
    relocate(SFPackOut, .after = TIExportKgs) |>
    left_join(SFResidAdjust |>
                select(c(Owner,NormPackOut,AdjustmentL,ResidAdjustedPred)), 
              by = "Owner") |>
    mutate(CRAPPO = alpha*ResidAdjustedPred,
           across(.cols = c(Bins,SFInputKgs,SFExportKgs), ~replace_na(.,0)))

  MasterSummaryByOwnerTotals <- MasterSummaryByOwner |>
    mutate(NormExportKgs = NormPackOut*SFInputKgs,
           RAPExportKgs = ResidAdjustedPred*SFInputKgs,
           CRAPExportKgs = CRAPPO*SFInputKgs) |>
    summarise(FieldBinsTipped = sum(FieldBinsTipped, na.rm=T),
              Bins = sum(Bins, na.rm=T),
              TIExportKgs = sum(TIExportKgs, na.rm=T),
              TIInputKgs = sum(TIInputKgs, na.rm=T),
              SFExportKgs = sum(SFExportKgs, na.rm=T),
              NormExportKgs = sum(NormExportKgs, na.rm=T),
              RAPExportKgs = sum(RAPExportKgs, na.rm=T),
              CRAPExportKgs = sum(CRAPExportKgs, na.rm=T),
              SFInputKgs = sum(SFInputKgs, na.rm=T)) |>
    mutate(TIPackOut = TIExportKgs/TIInputKgs,
           SFPackOut = SFExportKgs/SFInputKgs,
           NormPackOut = NormExportKgs/SFInputKgs,
           RAPPO = RAPExportKgs/SFInputKgs,
           CRAPPO = CRAPExportKgs/SFInputKgs) |>
    select(c(FieldBinsTipped,Bins,TIPackOut,SFPackOut,NormPackOut,RAPPO,CRAPPO)) |>
    mutate(Owner = "Total",
           AdjustmentL = NA) |>
    relocate(Owner, .before = FieldBinsTipped) |>
    relocate(AdjustmentL, .before = RAPPO)

#  
# Assembling the table for publishing this needs to include the RTEs also
#

  MasterSummaryTable <- MasterSummaryByOwner |>
    rename(RAPPO = ResidAdjustedPred) |>
    select(c(Owner,FieldBinsTipped,Bins,TIPackOut,SFPackOut,NormPackOut,AdjustmentL,RAPPO,CRAPPO)) |>
    bind_rows(MasterSummaryByOwnerTotals) |>
    mutate(AdjustmentL = replace_na(AdjustmentL, 0))

#
# Adding the RTEs
#

  MasterSummaryTableWithRTEs <- MasterSummaryTable |>
    left_join(RTEsByOwner, by = "Owner")

#write_csv(MasterSummaryTableWithRTEs, "AdjustedRTEsByGrower.csv")

  return(MasterSummaryTableWithRTEs)

}

SummaryByOwner <- SummaryByOwner(TrainingTable,TestTable,RTEsByOwner,alpha)
