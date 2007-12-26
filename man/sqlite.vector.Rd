\name{sqlite.vector}
\alias{sqlite.vector}
\title{ SQLite Vector }
\description{
Creates a SQLite Matrix (SVEC) from atomic vectors.
}
\usage{
sqlite.vector(vec, name = NULL)
}
\arguments{
  \item{vec}{ an atomic vector }
  \item{name}{ name of the SQLite Data Frame created which will hold the vector }
}
\details{
Creates an SDF with 1 column named V1 which will hold the data of the vector.
The \emph{mode} of the SQLite vector is determined from data.
}
\value{
A S3 object representing the SQLite Vector.
}
\author{Miguel A. R. Manese}
\note{ SQLite vectors are not limited to columns of the table \code{sdf_data}.
For example, with SQLite matrices the row names are exposed as SQLite vectors
on columns of a table other than \code{sdf_data}. }
\seealso{\code{\link[SQLiteDF]{sqlite.matrix}}
         \code{\link[SQLiteDF]{sqlite.data.frame}}}
\examples{
    data <- runif(30000)
    summary(data)
}
\keyword{data}
\keyword{manip}
\keyword{classes}
