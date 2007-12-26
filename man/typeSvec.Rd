\name{typeSvec}
\alias{typeSvec}
\alias{has.typeSvec}
\title{Get Type of sqlite.vector }
\description{
Returns the \emph{class} of an sqlite.vector if it were
}
\usage{
typeSvec(x)
has.typeSvec(x, type)
}
\arguments{
  \item{x}{an sqlite.vector}
  \item{type}{a string containing the class name, like numeric, factor, character}
}
\value{
  \item{typeSvec}{returns a string with the class name of the argument}
  \item{has.typeSvec}{returns TRUE if x is a sqlite.vector and has type as returned by typeSvec}
}
\author{Miguel A. R. Manese}
\examples{
    iris.sdf <- sqlite.data.frame(iris)
    typeSvec(iris.sdf[,1])   # numeric
    has.typeSvec(iris.sdf[,1], "numeric")  # TRUE
}
\keyword{classes}
