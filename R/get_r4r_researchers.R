# Get R4R Researcher Data for Review
# Stephen Howe
# 22 February 2025
# Version 2

# versions ----
# v1 20250221 - initial script
# v2 20250222 - added complete download of researcher docs in parquet

# packages ----
library(aws.s3)
library(arrow)
library(duckdb)
library(dplyr)
library(jsonlite)

da_proj <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/nodes/r4r_projection_authors/")
duckdb_register_arrow(con, "da_proj_2", da_proj)

# tet query
foo_test <- dbGetQuery(
  con,
  "SELECT * FROM da_proj_2 WHERE persistent_identifier = 'r4r:01JMF0251C5SSK0K5DXP9W2HGG'"
)



# downlaod projection ----
objects <- aws.s3::get_bucket_df(
  bucket = "com-copyright-agpipeline-sandds",
  prefix = "data/internal/parquet/r4r/phyeng/graph/nodes/r4r_projection_authors/"
)

getFiles <- function(x,y){
  save_object(
    object = x,
    bucket = "com-copyright-agpipeline-sandds",
    file = paste0("data/r4r_", y, ".parquet")
  )
}

# test function
# getFiles(
#   "data/internal/parquet/r4r/phyeng/graph/nodes/r4r_projection_authors/part-00000-bc00116c-17a7-47e1-846b-1b6e171f616c-c000.snappy.parquet",
#   "001"
# )

# run function en masse
my_list <- objects$Key
my_numbers <-rep(paste0("proj_", 1:nrow(objects)))

mapply(getFiles, my_list, my_numbers)

# duckdb connection to parquet docs
con <- dbConnect(duckdb::duckdb())
dbExecute(con, "CREATE TABLE da_proj AS SELECT * FROM read_parquet('data/*.parquet')")


# data slices ----
top_10 <- dbGetQuery(
  con,
  "SELECT *
  FROM da_proj
  ORDER BY occurrences DESC
  LIMIT 10"
)

top_collab <- dbGetQuery(
  con,
  "SELECT *
  FROM da_proj
  ORDER BY colla"
)






### SCRATCH ####
my_parquet <- list.files("data/",
                         full.names = TRUE)
data_dir <- "/Users/stephenhowe/Documents/code_active/r4r_dq_review/data"
da_proj <-- open_dataset(
  data_dir,
  format = "parquet"
)

read_parquet(my_parquet[[1]])
# da <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/nodes/distinctauthor")
# da_proj <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/nodes/r4r_projection_authors/")
# # au_docs <- open_dataset("s3://com-copyright-agpipeline-sandds/data/output/phyeng/json/graph/nodes/", format = "json")
#
#
#
#
# # save for app ----
#
# getSaveJson <- function(x){
#   da_proj |>
#     filter(persistent_identifier == x) |>
#     collect()
# }
#
#
#
# # test and scratch
# foo_test <- getSaveJson("r4r:01JMF0251C5SSK0K5DXP9W2HGG")
