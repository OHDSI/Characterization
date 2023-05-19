context("Database")

test_that("removeMinCell", {
data <- data.frame(name = rep('dfd', 10), val = 1:10)
newData <- removeMinCell(
  data = data, minCellCount = 5, minCellCountColumns = list()
)
testthat::expect_equal(newData, data)

newData <- removeMinCell(
  data = data, minCellCount = 5, minCellCountColumns = list('val')
)
testthat::expect_equal(newData$val[5:10], data$val[5:10])
testthat::expect_equal(newData$val[1:4], rep(-1,4))

newData <- removeMinCell(
  data = data, minCellCount = 50, minCellCountColumns = list('val')
)
testthat::expect_equal(newData$val, rep(-1,10))

data <- data.frame(
  name = rep('dfd', 10),
  val = 1:10,
  val2 = c(1,10,1,10,1,10,1,10,1,10)
)
newData <- removeMinCell(
  data = data, minCellCount = 5, minCellCountColumns = list('val')
)
testthat::expect_equal(newData$val[1:4], rep(-1,4))
testthat::expect_equal(newData$val2, data$val2,)

newData <- removeMinCell(
  data = data, minCellCount = 5, minCellCountColumns = list(c('val','val2'))
)
testthat::expect_equal(
  sum(
    (newData$val > 0 & newData$val < 5) | (newData$val2 > 0 & newData$val2 < 5)
  ),
  0
)

data <- data.frame(
  name = rep('dfd', 10),
  val = 1:10,
  val2 = c(1,10,1,10,1,10,1,10,1,10),
  val3 = c(10,10,10,10,10,1,1,10,10,10)
)
newData <- removeMinCell(
  data = data,
  minCellCount = 5,
  minCellCountColumns =
    list(c('val','val2'), 'val3')
)
testthat::expect_equal(
  sum(
    (newData$val > 0 & newData$val < 5) |
      (newData$val2 > 0 & newData$val2 < 5) |
      (newData$val3 > 0 & newData$val3 < 5)
  ),
  0
)

})
