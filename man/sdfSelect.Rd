\name{sdfSelect}
\alias{sdfSelect}
\title{ Directly Query an SQLite Data Frame }
\description{
Directly query an SQLite Data Frame using a SELECT statement.
}
\usage{
sdfSelect(sdf, select = NULL, where = NULL, limit = NULL, debug = FALSE)
}
\arguments{
  \item{sdf}{ the sqlite.data.frame }
  \item{select}{ content of the SELECT clause. If NULL, then "*" is assumed }
  \item{where}{ content of the WHERE clause. If NULL, then an empty 
        WHERE clause is assumed }
  \item{limit}{ content of the LIMIT clause. This limits the rows returned }
  \item{debug}{ if true, prints the SQL statement issued to SQLite }
}
\details{
Issues a SELECT statement to the corresponding data table of the passed SDF.
It forms the actual SELECT statement from the fragments supplied as arguments. 
This insulates the user from the underlying naming conventions and database
organization used by the package. To do more sophisticated queries, please
use RSQLite and open the databases in the \code{.SQLiteDF} directory under
your working directory.

Use square brackets to quote column names that are not valid SQL object names.
E.g. to select the Petal.Length column of a SDF copy of dataset iris, use
[Petal.Length].

The limit clause is a way to restrict the results of a query. Note that the
limiting operation is done after the resultset has been determined, and in
no way can effect any computation in the select clause. E.g. \code{select 
count(*) from sdf_data limit 4,10} will not return 10. It will return an
empty set since the result of the SELECT statement without the LIMIT clause
is a single row, and the LIMIT clause takes 10 rows starting from the 5th
row in the result set.
}
\value{
Returns NULL if the query does not return any row, a vector of the 
appropriate class (for factors) if there is only
one column selected, or a data frame if there are more than one columns.
}
\references{ ~put references to the literature/web site here ~ }
\author{ Miguel A. R. Manese }
\examples{
    iris.sdf <- sqlite.data.frame(iris)
    sdfSelect(iris.sdf, "[Petal.Length]", "[Petal.Length]>3")
    sdfSelect(iris.sdf, where="[Petal.Length]>3", limit="9,5")
    sdfSelect(iris.sdf, where="Species=3")
}
\keyword{data}
\keyword{manip}
