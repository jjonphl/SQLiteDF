\name{lsSdf}
\alias{lsSdf}
\title{ List SDF's}
\description{
Lists the SDF's available in the workspace.
}
\usage{ lsSdf(pattern = NULL) }
\arguments{
  \item{pattern}{Pattern of the internal name to be searched. Currently not implemented.}
}
\details{
\code{lsSdf} works much like \code{ls} for examining the contents of R's workspace.
It does this by querying the list of SDF's stored in \code{workspace.db} file.
See \code{\link[SQLiteDF]{sqlite.data.frame}} for more details on \code{workspace.db}.
}
\value{
Returns in a character vector the internal names of SDF's \emph{registered} with
the SQLiteDF workspace. The internal names are sorted according to (1) those that
are attached first and then (2) the usage score.
}
\author{Miguel A. R. Manese}
\seealso{
    \code{\link[SQLiteDF]{sqlite.data.frame}}
    \code{\link[SQLiteDF]{lsSdf}}
    \code{\link[SQLiteDF]{attachSdf}}
    \code{\link[SQLiteDF]{detachSdf}}
    \code{\link[SQLiteDF]{renameSdf}}
}
\examples{
    lsSdf()
}
\keyword{data}
