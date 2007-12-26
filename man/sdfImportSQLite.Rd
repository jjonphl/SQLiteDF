\name{sdfImportSQLite}
\alias{sdfImportSQLite}
\title{Import SQLite data to an SQLite Data Frame}
\description{
Import from SQLite tables directly.
}
\usage{
sdfImportSQLite(dbfilename, tablename, iname = tablename)
}
\arguments{
  \item{dbfilename}{file name of the SQLite database.}
  \item{tablename}{name of the table inside the SQLite database that will be imported}
  \item{iname}{the internal name of the created SDF}
}
\value{
The SDF containing the imported data.
}
\author{Miguel Angel R. Manese}
\note{
Row names is just the sequence from 1 to number of rows.
Text columns are converted into factors. Blobs are not supported.  }
\seealso{ \code{\link[SQLiteDF]{sdfImportDBI}} }
\examples{
\dontrun{
data.sdf <- sdfImportSQLite("data.db", "fuel_frame", iname="fuel_frame")
}
}
\keyword{database}
