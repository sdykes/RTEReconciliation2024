library(tidyverse)

SummaryByGB <- function(TrainingTable, TestTable, SFResidAdjust, MappedInputBins, RTEsByPoolWithBulkKgs) {

  TIRawGB <- TrainingTable |>
    mutate(ExportKgs = PackOut*InputKgs) |>
    select(c(GraderBatchID, FarmCode, Orchard, ProductionSite, Owner, 
             FieldBinsTipped, InputKgs, ExportKgs, PackOut)) |>
    rename(Bins = FieldBinsTipped) |>
    mutate(NormExportKgs = 0,
           NormSFPackOut = NA,
           RAPPO = NA,
           CRAPPO = NA,
           FinalPackOut = PackOut,
           Source = "Te Ipu")

#
# Sunfruit Packouts by GraderBatch
#

  SFRawGB <- SFPackRunWithNormalisedPO |>
    left_join(MappedInputBins |>
                select(Run, GraderBatchID),
              by = "Run") |>
    mutate(ExportKgs = PackOut*InputKgs,
           NormExportKgs = .pred*InputKgs) |>
    group_by(GraderBatchID, FarmCode, Orchard, ProductionSite, Owner) |>
    summarise(Bins = sum(Bins),
              InputKgs = sum(InputKgs),
              ExportKgs = sum(ExportKgs),
              NormExportKgs = sum(NormExportKgs),
              .groups = "keep") |>
    mutate(PackOut = ExportKgs/InputKgs,
           NormSFPackOut = NormExportKgs/InputKgs) |>
    relocate(PackOut, .after = ExportKgs) |>
# Add the residual adjustment
    left_join(SFResidAdjust |>
                select(c(Owner, RAPFactor)),
                       by = "Owner") |>
    mutate(RAPPO = NormSFPackOut*RAPFactor,
           CRAPPO = alpha*RAPPO,
           FinalPackOut = CRAPPO,
           Source = "Sunfruit Limited") |>
    select(-c(RAPFactor))

#
# Combine the two (SF and TI) together 
#

  CombinedBatches <- TIRawGB |>
    bind_rows(SFRawGB) |>
    arrange(GraderBatchID)

#
# Add the unadjusted RTEs
#

  CombinedBatchedWithRTEs <- CombinedBatches |>
    mutate(PackoutDiff = PackOut-FinalPackOut) |>
    left_join(RTEsByPoolWithBulkKgs, by = "GraderBatchID") |>
    mutate(`53A` = `53`*(1-PackoutDiff),
           `58A` = `58`*(1-PackoutDiff),
           `63A` = `63`*(1-PackoutDiff),
           `67A` = `67`*(1-PackoutDiff),
           `72A` = `72`*(1-PackoutDiff),
           BulkA = Bulk*(1-PackoutDiff),
           BulkKgsA = BulkKgs*(1-PackoutDiff))

  return(CombinedBatchedWithRTEs)
  
}

SUmmaryByGB <- SummaryByGB(TrainingTable, TestTable, SFResidAdjust, MappedInputBins, RTEsByPoolWithBulkKgs)

