if(Sys.getenv('GITHUB_ACTIONS') == 'true') {
  actions <- F
} else{
  actions <- F
}

connectionDetails <- Eunomia::getEunomiaConnectionDetails()
Eunomia::createCohorts(connectionDetails = connectionDetails)
