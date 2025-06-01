library(arrow)
library(duckdb)
library(dplyr)
library(dbplyr)

openalex <- open_dataset(
  "/Users/stephenhowe/Documents/data/csv_openalex",
  format = "csv"
)

openalex |>
  head() |>
  collect()

con <- dbConnect(duckdb::duckdb())
arrow::to_duckdb(openalex, table_name = "openalex", con = con)

foo <- dbGetQuery(
  con,
  "Select * from openalex limit 10"
)

dbGetQuery(con, "select count(id) from openalex")


# extract all the author ids and dois
# set core utilization in DuckDB
DBI::dbExecute(con, "PRAGMA threads=10")

dbExecute(
  con,
  "COPY (
  SELECT
  doi,
  author_id
  FROM (
    SELECT
    REGEXP_EXTRACT(id, 'doi:[^ ]+') AS doi,
    REGEXP_EXTRACT_ALL(author, 'omid:ra/\\d+') AS author_ids
    FROM openalex
  ) t,
  UNNEST(author_ids) AS author_id
) TO '/Users/stephenhowe/Desktop/doi_author_pairs.csv' (FORMAT 'csv');"
)

# 15:10 start time
Sys.time() # [1] "2025-05-30 15:17:59 EDT"

# 15:19 for .csv start time
# End: [1] "2025-05-30 15:27:57 EDT"

results <- arrow::open_csv_dataset("/Users/stephenhowe/Desktop/doi_author_pairs.csv")

results |> count() |> collect()

fugazi_1 <- results |> head(10) |> collect()


# this runs:
t <- Sys.time()
dbExecute(
  con,
  "COPY (
  SELECT
    doi,
    TRIM(REGEXP_EXTRACT(pair.unnest, '^(.*?) \\[omid:ra/\\d+\\]')) AS author_name,
    REGEXP_EXTRACT(pair.unnest, 'omid:ra/\\d+') AS author_id
  FROM (
    SELECT
      REGEXP_EXTRACT(id, 'doi:[^ ]+') AS doi,
      REGEXP_EXTRACT_ALL(author, '[^;\\[]+\\[omid:ra/\\d+\\]') AS pairs
    FROM openalex
  ) t,
  UNNEST(pairs) AS pair
) TO '/Users/stephenhowe/Desktop/doi_author_pairs.parquet' (FORMAT 'parquet');"
)
Sys.time() - t

results_2 <- open_csv_dataset("/Users/stephenhowe/Desktop/doi_author_pairs.csv")
fugazi_2<- results_2 |> head(100) |> collect()
results_2 |> count() |> collect()

results_3 <- open_dataset("/Users/stephenhowe/Desktop/doi_author_pairs.parquet")
fugazi_3<- results_3 |> head(100) |> collect()
results_3 |> count() |> collect()


# Getting RR euiqvalent data
library(aws.s3)
library(arrow)
library(duckdb)
library(dplyr)

con <- dbConnect(duckdb::duckdb())

# contribution edge
contributed_to <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/edges/CONTRIBUTED_TO/")
to_duckdb(contributed_to, table_name = "contributed_to", con = con)

# author info
# da <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/nodes/distinctauthor")
# duckdb_register_arrow(con, "distinctAuthor", da)

# test
fugazi_1 <- dbGetQuery(
  con,
  "Select
    ct.target as doi,
    SUBSTR(ct.source, 1, 40) AS author_name,
    ct.source as author_id
  from contributed_to ct
  limit 10"
) # works

dbExecute(
  con,
  "COPY (
  SELECT
    ct.target as doi,
    SUBSTR(ct.source, 1, 40) AS author_name,
    ct.source as author_id
  from contributed_to ct
  ) TO '/Users/stephenhowe/Documents/code_active/r4r_dq_review/aap_acp_k_b3/r4r.parquet' (FORMAT 'parquet');"
)


# r4r:00018dd7-909b-435f-bade-6be4b8b8bbd9_rinne_mikael

# r4r:0043e9de-08f1-4f38-a4cf-6b968a761157_e_mcgrath_c
