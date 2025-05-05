#
#  Rev 2.0 this has been modified to use the post residual adjusted packouts
#

DeltaCalibration <- function(TrainingData, TestData, ResidualData) {
  
  TeIpuPackOut <- TrainingData |>
    mutate(ExportKgs = PackOut*InputKgs) |>
    summarise(InputKgs = sum(InputKgs, na.rm=T),
              ExportKgs = sum(ExportKgs, na.rm=T)) |>
    mutate(PackOut = ExportKgs/InputKgs)
  
  SunFruitActualPackOut <- TestData |>
    mutate(ExportKgs = PackOut*InputKgs) |>
    summarise(InputKgs = sum(InputKgs, na.rm=T),
              ExportKgs = sum(ExportKgs, na.rm=T)) |>
    mutate(PackOut = ExportKgs/InputKgs)
  
  ActualPackOutDiff <- SunFruitActualPackOut$PackOut[[1]] - TeIpuPackOut$PackOut[[1]]
  
  alpha <- 1.0
  
  SunFruitNormPackOut <- ResidualData |>
    mutate(RAPExportKgs = RAPAdjustedPO*SFInputKgs) |>
    select(c(RAPAdjustedPO, RAPExportKgs, SFInputKgs)) 
  
  SunFruitNormPackOutSummary <- SunFruitNormPackOut |>
    summarise(SFInputKgs = sum(SFInputKgs),
              RAPExportKgs = sum(RAPExportKgs)) |>
    mutate(RAPPackOut = RAPExportKgs/SFInputKgs)
  
  NormPackOutDiff <- SunFruitActualPackOut$PackOut[[1]] - SunFruitNormPackOutSummary$RAPPackOut[[1]]
  
  AlphaSolve <- function(alpha) {
    SunFruitActualPackOut$PackOut[[1]] - 
      (alpha*sum(SunFruitNormPackOut$RAPExportKgs)/SunFruitNormPackOutSummary$SFInputKgs) -
      ActualPackOutDiff 
  }
  
  alpha2 <- uniroot(AlphaSolve, c(0.9,1.2))$root
  
  return(alpha2)
  
}

alpha <- DeltaCalibration(lr_FinalTrainingTable, SFPackRunWithNormalisedPO, SFResidAdjustGB)

SunFruitNormPackOut <- SFPackRunWithNormalisedPO |>
  rename(NormPackOut = .pred) |>
  mutate(PredExportKgs = NormPackOut*InputKgs,
         CaliPredExportKgs = PredExportKgs*alpha,
         CaliNormPackOut = CaliPredExportKgs/InputKgs) 

#
# Summarise all pack runs
#

SunFruitNormPackOutSummary <- SunFruitNormPackOut |>
  mutate(ExportKgs = PackOut*InputKgs) |>
  summarise(InputKgs = sum(InputKgs),
            ExportKgs = sum(ExportKgs),
            PredExportKgs = sum(PredExportKgs),
            CaliPredExportKgs = sum(CaliPredExportKgs)) |>
  mutate(ActualPackOut = ExportKgs/InputKgs,
         NormPackOut = PredExportKgs/InputKgs,
         CaliNormPackOut = CaliPredExportKgs/InputKgs) |>
  mutate(Owner = "Total") |>
  relocate(Owner, .before = InputKgs)

#
# Normalised and calibrated packout by Onwer
#

SFNPackOutByOwner <- SunFruitNormPackOut |>
  mutate(ExportKgs = PackOut*InputKgs) |>
  group_by(Owner) |>
  summarise(InputKgs = sum(InputKgs),
            ExportKgs = sum(ExportKgs),
            PredExportKgs = sum(PredExportKgs),
            CaliPredExportKgs = sum(CaliPredExportKgs)) |>
  mutate(ActualPackOut = ExportKgs/InputKgs,
         NormPackOut = PredExportKgs/InputKgs,
         CaliNormPackOut = CaliPredExportKgs/InputKgs) 

#
# Summarise all pack runs by GraderBatchID  
#

SunFruitNormPackOutSummaryGBID <- SunFruitNormPackOut |>
  mutate(ExportKgs = PackOut*InputKgs) |>
  summarise(InputKgs = sum(InputKgs),
            ExportKgs = sum(ExportKgs),
            PredExportKgs = sum(PredExportKgs),
            CaliPredExportKgs = sum(CaliPredExportKgs)) |>
  mutate(ActualPackOut = ExportKgs/InputKgs,
         NormPackOut = PredExportKgs/InputKgs,
         CaliNormPackOut = CaliPredExportKgs/InputKgs) |>
  mutate(GraderBatchID = "Total") |>
  relocate(GraderBatchID, .before = InputKgs)

#
# Attached the Grader Batches to the packruns 
#

SunFruitNormPackOutByGBID <- SFPackRunWithNormalisedPO |>
  rename(NormPackOut = .pred) |>
  mutate(PredExportKgs = NormPackOut*InputKgs,
         CaliPredExportKgs = PredExportKgs*alpha,
         CaliNormPackOut = CaliPredExportKgs/InputKgs) |>
  left_join(MappedInputBins |> select(c(Run, GraderBatchID)),
            by = "Run") |>
  mutate(ExportKgs = PackOut*InputKgs) |>
  group_by(GraderBatchID) |>
  summarise(InputKgs = sum(InputKgs),
            ExportKgs = sum(ExportKgs),
            PredExportKgs = sum(PredExportKgs),
            CaliPredExportKgs = sum(CaliPredExportKgs)) |>
  mutate(ActualPackOut = ExportKgs/InputKgs,
         NormPackOut = PredExportKgs/InputKgs,
         CaliNormPackOut = CaliPredExportKgs/InputKgs,
         GraderBatchID = as.character(GraderBatchID)) |>
  bind_rows(SunFruitNormPackOutSummaryGBID)


  






