\name{sdflm}
\alias{sdflm}
\title{ Linear Models On SQLite Data Frames}
\description{
Currently, a convenience wrapper around biglm. In the future,
biglm functionalities will be implemented directly inside the package.
}
\usage{
sdflm(formula, sdf, batch.size = 1024)
}
\arguments{
  \item{formula}{model formula}
  \item{sdf}{ the sqlite.data.frame }
  \item{batch.size}{number of rows at a time to be \emph{fed} to biglm }
}
\details{
biglm can computes the linear model coefficients and statistics incrementally
by \emph{feeding} it with part of the data first at a time. \code{sdflm} just
gets data from the SDF into a plain data frame, \code{batch.size} at a time,
and then used with \code{biglm}. to 

Currently, models using only those variables inside the SDF are supported (i.e.
do not use variables that is not in the SDF, like those in the global environment). 
}
\value{
  An object of class biglm.
}
\author{ Miguel A. R. Manese }
\seealso{ \code{\link[biglm]{biglm}}}
\examples{
library(biglm)
iris.sdf <- sqlite.data.frame(iris)
iris.lm <- sdflm(Sepal.Length ~ Species, iris.sdf)
summary(iris.lm)
}
\keyword{regression}% at least one, from doc/KEYWORDS
