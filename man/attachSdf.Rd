\name{attachSdf}
\alias{attachSdf}
\title{ Attach SDF Database }
\description{
Attaches a SDF to the workspace by adding to the SDF list in \code{workspace.db}
and then attaching the SDF file in the SQLite engine.
}
\usage{
attachSdf(sdf_filename, sdf_iname = NULL)
}
\arguments{
  \item{sdf_filename}{ A string containing a path (recommended is relative path) to the SDF database file. }
  \item{sdf_iname}{ A string containing an internal name with which the SDF to be attached
will be renamed. By default, the internal name stored in the SDF's attribute table will be used
as the internal name. Duplicates will be resolved by appending numbers. }
}
\details{
Attaching a SDF into the SQLiteDF workspace involves adding an entry in \code{workspace.db}
and then attaching to the SQLite engine. Checks are made to avoid attaching the same file twice.
This is done by storing the full path name of the SDF file in \code{workspace.db}. When loading
SQLiteDF, the relative path is used to locate the SDF files and then the full path are recalculated.
When a SDF file is to be attached with \code{attachSdf}, its full path is calculated and then
it is compared with those in the workspace. These restrictions are to avoid possibly subtle problems.
(In retrospect, I can't imagine any serious problem yet but since it's there already... To think
that I cringed for a week on this problem, I \emph{am} an idiot.)
}
\value{
Returns an sqlite.data.frame object of the attached SDF if successful. \code{NULL}
with an error message displayed otherwise. 
}
\author{Miguel A. R. Manese}
\seealso{
    \code{\link[SQLiteDF]{sqlite.data.frame}}
    \code{\link[SQLiteDF]{attachSdf}}
    \code{\link[SQLiteDF]{lsSdf}}
}
\keyword{data}
