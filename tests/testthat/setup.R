library(Eunomia)
connectionDetails <- getEunomiaConnectionDetails()
connection <- DatabaseConnector::connect(connectionDetails)

withr::defer({
  DatabaseConnector::disconnect(connection)
  unlink(connectionDetails$server())
}, testthat::teardown_env())
