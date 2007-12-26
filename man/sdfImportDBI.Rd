\name{sdfImportDBI}
\alias{sdfImportDBI}
\title{ Import Data to an SQLite Data Frame }
\description{
Import data from a DBMS to a sqlite.data.frame using DBI.
}
\usage{
sdfImportDBI(con, sql, batch.size = 2048, rownames = "row_names", iname = NULL)
}
\arguments{
  \item{con}{a DBI connection object }
  \item{sql}{a select statement to retrieve the data to be imported}
  \item{batch.size}{the number of rows to be fetched from the connection at a time}
  \item{rownames}{a string or index specifying the column in the result of the select
    statement to be used as the row.names of the SDF}
  \item{iname}{the internal name of the created SDF}
}

\details{
This function just sends the query to the connection (\code{DBI:::dbSendQuery})
and then loops over the result set of the select statement (\code{DBI:::fetch}).
}
\value{
The SDF containing the imported data.
}
\author{Miguel A. R. Manese}
\seealso{ \code{\link[DBI]{dbSendQuery}}
          \code{\link[DBI]{fetch}}
          \code{\link[DBI]{dbReadTable}}}
\examples{
\dontrun{
conn <- dbConnect("MySQL", group = "vitalAnalysis")
fuel_frame.sdf <- sdfImportDBI(conn, "select * from fuel_frame", iname="fuel_frame")
}
}
\keyword{database}
