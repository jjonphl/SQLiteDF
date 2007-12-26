\name{sdflm2}
\alias{sdflm2}
\title{Linear Models on SQLite Data Frames}
\description{
biglm specialized for SQLite Data Frames
}
\usage{
sdflm2(x, y, intercept = TRUE)
}
\arguments{
  \item{x}{ a SQLite data frame containing the design matrix which may not include the intercept }
  \item{y}{ a SQLite vector containing the observed response }
  \item{intercept}{ if TRUE, adds an intercept term when doing computation }
}
\details{
Algorithm is identical with \code{biglm}. The only difference is that the rows of \code{x}
and the values of \code{y} are directly fed to the algorithm.
}
\value{Returns a subclass of \code{biglm}. \code{biglm} methods can be used with the
output, e.g. compute coefficients, vcov, etc.
}
\references{Algorithm AS274  Applied Statistics (1992) Vol.41, No. 2 }
\author{Miguel A. R. Manese}
\seealso{ \code{\link[biglm]{biglm} } }
\examples{
    library(biglm)
    iris.sdf <- sqlite.data.frame(iris)
    x <- iris.sdf[,1:3]
    y <- iris.sdf[,4]
    iris.biglm <- sdflm2(x, y)
    summary(iris.biglm)
}
\keyword{regression}
