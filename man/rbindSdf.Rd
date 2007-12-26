\name{rbindSdf}
\alias{rbindSdf}
\title{Add Rows to a SDF} 
\description{
Adds rows to a SQLite data frame. 
}
\usage{
rbindSdf(sdf, df)
}
\arguments{
  \item{sdf}{the SQLite data frame with which to insert new data. }
  \item{df}{the data frame to attach}
}
\details{
This is equivalent to performing an \code{insert} statement. The
data frame names and the column types must match, although they
may be not arranged exactly the same. 
}
\value{
Returns the 1st argument (sdf).
}
\author{Miguel A. R. Manese}
\note{
Currently SDFs are not supported as the 2nd arg. This could
be achieved by doing batch rbindSdf to chunks of another SDF.
}
\examples{
    b1 <- sqlite.data.frame(iris)
    rbindSdf(b1, iris)
    nrow(b1)  # nrow(iris) * 2
}
\keyword{manip}
