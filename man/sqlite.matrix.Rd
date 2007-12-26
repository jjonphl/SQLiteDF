\name{sqlite.matrix}
\alias{sqlite.matrix}
\title{SQLite Matrix}
\description{
EXPERIMENTAL: Creates a SQlite Matrix (SMAT) from data frames, matrices
and SQLite Data Frames.
}
\usage{
sqlite.matrix(data, name = NULL)
}
\arguments{
  \item{data}{ a data frame, matrix or SQLite Data Frame }
  \item{name}{ the name of the SQLite Matrix }
}
\details{
Creates an SDF with 1 column named V1. The \emph{mode} of the matrix
is determined from the data, much like \code{as.matrix} does. It
is similar to a \code{sqlite.vector} and has the additional attributes
\code{sdf.dim} and \code{sdf.dimnames} comparable to \code{matrix}'s
\code{dim} and {dimnames} attributes.

Internally, it has extra tables \emph{sdf\_matrix\_rownames} which holds
the row names (and implicitly the row count), and \emph{sdf\_matrix\_colnames} 
which holds the column names (and implicitly the column count).
}
\value{
A S3 object representing the SQLite Matrix.
}
\author{Miguel A. R. Manese}
%\note{ }
\seealso{\code{\link[SQLiteDF]{sqlite.vector}}
         \code{\link[SQLiteDF]{sqlite.data.frame}}}
\examples{
    iris.sdf <- sqlite.data.frame(iris)
    im <- sqlite.matrix(iris.sdf[,1:4])
    dim(im)   # c(150, 4)
}
\keyword{data}
\keyword{manip}
\keyword{classes}
