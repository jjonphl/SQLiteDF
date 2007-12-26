\name{getSdf}
\alias{getSdf}
\title{ Get an SDF }
\description{
Gets a handle to an SDF registered in the workspace. The returned
handle is a sqlite.data.frame object which can be used with the most 
common operators for data frames.
}
\usage{ getSdf(name) }
\arguments{ 
\item{name}{ A string containing the internal name of the desired SDF. This SDF
is attached to the SQLiteDF workspace if it is not yet attached.}
}
\details{
The SDF must be \emph{registered} with the SQLiteDF workspace. To get a list
of registered SDF, use \code{lsSdf}.
}
\value{ Returns an sqlite.data.frame object for the SDF with the specified
internal name.  }
\author{Miguel A. R. Manese}
\seealso{ 
    \code{\link[SQLiteDF]{sqlite.data.frame}}
    \code{\link[SQLiteDF]{lsSdf}}
}
\examples{
    iris.sdf <- sqlite.data.frame(iris)
    iris.sdf.iname <- inameSdf(iris.sdf)
    iris.sdf.too <- getSdf(iris.sdf.iname)
}
\keyword{data}
