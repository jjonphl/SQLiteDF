\name{detachSdf}
\alias{detachSdf}
\title{ Detach SDF }
\description{
Detaches a SDF from the workspace by detaching from the SQLite engine
and then deleting from the SDF list in \code{workspace.db}.
}
\usage{
detachSdf(iname)
}
\arguments{
  \item{iname}{ A string containing the internal name of the desired SDF. }
}
\value{
\code{TRUE} if the operation was successful. \code{FALSE} with an error
message displayed otherwise.
}
\author{Miguel A. R. Manese}
\seealso{
    \code{\link[SQLiteDF]{sqlite.data.frame}}
    \code{\link[SQLiteDF]{attachSdf}}
    \code{\link[SQLiteDF]{lsSdf}}
}
\keyword{data}
