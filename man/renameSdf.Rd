\name{renameSdf}
\alias{renameSdf}
\title{ Rename SDF }
\description{
Changes the internal name of an SDF, both in the workspace and in the 
SDF's attributes table.
}
\usage{
renameSdf(sdf, name)
}
\arguments{
  \item{sdf}{ The SDF to be renamed. }
  \item{name}{ The new internal name of the SDF. }
}
\details{
This simple operation have a quite complicated implementation. The intended
name is checked against SDF's already registed to the workspace, the SDF
is detached and removed from the SDF workspace, and then re-attached and
re-added to the workspace. }
\value{
\code{TRUE} if the operation was successful. \code{FALSE} with an error
message displayed otherwise.
}
\author{Miguel A. R. Manese}
\seealso{
    \code{\link[SQLiteDF]{lsSdf}}
}
\keyword{data}
