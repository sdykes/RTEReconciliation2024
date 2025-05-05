library(tidyverse)
library(tidymodels)

#
# Put the model in a function
#
MLModel <- function(FinalTrainingTable) {

#
# Create cross-validation sets
#
  PO_lr_fold <- vfold_cv(FinalTrainingTable, v = 10, strata = MaturityCode)
#
# Develop recipe for the lasso regression 
#
  lasso_rec <- 
    recipe(PackOut ~ ., data = FinalTrainingTable) |> 
    step_novel(MaturityCode) |>
    step_unknown() |>
    step_dummy(all_nominal_predictors()) |>
    step_zv(all_predictors()) |>
    step_normalize(all_predictors())
#
# Specifiy the model i.e. mixture = 100% Lasso
#
  lasso_spec <- 
    linear_reg(penalty = tune(), mixture = tune()) |>
    set_mode("regression") |>
    set_engine("glmnet")
#
# specify for the workflow for tuning
#
  lasso_workflow <- workflow() |>
    add_recipe(lasso_rec) |>
    add_model(lasso_spec) 

  penalty_grid <- grid_regular(penalty(), mixture(), levels = 10) 

  tune_res <- tune_grid(
    lasso_workflow,
    resamples = PO_lr_fold,
    grid = penalty_grid
  )  

#autoplot(tune_res)

  best_penalty <- select_best(tune_res, metric = "rmse")

  lasso_final <- finalize_workflow(lasso_workflow, best_penalty)

  lasso_final_fit <- fit(lasso_final, data = FinalTrainingTable)

  return(lasso_final_fit)
    
}

lasso_final_fit <- MLModel(FinalTrainingTable)

# Training data with modeled packout

lr_FinalTrainingTable <- TrainingTable |>
  bind_cols(predict(lasso_final_fit, new_data = TrainingTable)) |>
  mutate(model = "lasso regression")

# Model applied to the test data

SFPackRunWithNormalisedPO <- TestTable |>
  rename(`Storage type` = StorageType) |>
  bind_cols(lasso_final_fit |> predict(new_data = TestTable |> rename(`Storage type` = StorageType)))

#write_csv(SFPackRunWithNormalisedPO,"SF2024PackRunsWithNormPO.csv")







