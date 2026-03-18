# code to combine covariate settings
combineCovariateSettingsJsons <- function(covariateSettingsJsonList) {
  # get unique
  covariateSettingsJsonList <- unique(covariateSettingsJsonList)

  # first convert from json
  covariateSettings <- lapply(
    X = covariateSettingsJsonList,
    FUN = function(x) {
      ParallelLogger::convertJsonToSettings(x)
    }
  )

  # then combine the covariates
  singleSettings <- which(unlist(lapply(covariateSettings, function(x) inherits(x, "covariateSettings"))))
  multipleSettings <- which(unlist(lapply(covariateSettings, function(x) inherits(x, "list"))))

  covariateSettingList <- list()
  if (length(singleSettings) > 0) {
    for (i in singleSettings) {
      covariateSettingList[[length(covariateSettingList) + 1]] <- covariateSettings[[i]]
    }
  }
  if (length(multipleSettings) > 0) {
    for (i in multipleSettings) {
      settingList <- covariateSettings[[i]]
      for (j in 1:length(settingList)) {
        if (inherits(settingList[[j]], "covariateSettings")) {
          covariateSettingList[[length(covariateSettingList) + 1]] <- settingList[[j]]
        } else {
          message("Incorrect covariate settings found") # stop?
        }
      }
    }
  }

  # check for covariates with same id but different
  endDays <- unique(unlist(lapply(covariateSettingList, function(x) {
    x$endDays
  })))
  if (length(endDays) > 1) {
    stop("Covariate settings for aggregate covariates using different end days")
  }
  longTermStartDays <- unique(unlist(lapply(covariateSettingList, function(x) {
    x$longTermStartDays
  })))
  if (length(longTermStartDays) > 1) {
    stop("Covariate settings for aggregate covariates using different longTermStartDays")
  }
  mediumTermStartDays <- unique(unlist(lapply(covariateSettingList, function(x) {
    x$mediumTermStartDays
  })))
  if (length(mediumTermStartDays) > 1) {
    stop("Covariate settings for aggregate covariates using different mediumTermStartDays")
  }
  shortTermStartDays <- unique(unlist(lapply(covariateSettingList, function(x) {
    x$shortTermStartDays
  })))
  if (length(shortTermStartDays) > 1) {
    stop("Covariate settings for aggregate covariates using different shortTermStartDays")
  }

  # convert to json
  covariateSettingList <- as.character(ParallelLogger::convertSettingsToJson(covariateSettingList))
  return(covariateSettingList)
}
