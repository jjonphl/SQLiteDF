\name{inameSdf}
\alias{inameSdf}
\title{SDF Internal Name}
\description{
Get the internal name of an SDF.
}
\usage{
inameSdf(sdf)
}
\arguments{
  \item{sdf}{a sqlite.data.frame}
}
\details{
Internal names are those that are listed in \code{lsSdf}, not the names of the
R variables holding a \emph{reference} to SDFs. There could be many R variable
that refers to the same SDF (when \code{inameSdf(sdf1) == inameSdf(sdf2)}).
}
\value{
A 2 element string vector with the internal name of the SDF and the SQLite database file name
where it is stored.
}
\author{Miguel A. R. Manese}
\seealso{
    \code{\link[SQLiteDF]{sqlite.data.frame}}
    \code{\link[SQLiteDF]{lsSdf}}
}
\examples{
iris.sdf1 <- sqlite.data.frame(iris)  
inameSdf(iris.sdf1) # c("data1", "data1.db"), or generally data<n>
iris.sdf2 <- sqlite.data.frame(iris, "iris")
inameSdf(iris.sdf2) # c("iris", "iris.db"), or if it already exists, iris<n>
}
\keyword{methods}
