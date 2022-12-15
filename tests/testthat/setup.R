if(Sys.getenv('GITHUB_ACTIONS') == 'true') {
  actions <- T
} else{
  actions <- F
}

connectionDetails <- Eunomia::getEunomiaConnectionDetails()
Eunomia::createCohorts(connectionDetails = connectionDetails)
