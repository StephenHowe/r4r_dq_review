library(aws.s3)
library(arrow)
library(dplyr)

contributed_to <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/edges/CONTRIBUTED_TO/")

contributed_to |>
  head() |>
  collect() |>
  View()

r4r:00018dd7-909b-435f-bade-6be4b8b8bbd9_rinne_mikael

contribution <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/edges/CONTRIBUTION/")

contribution |>
  head() |>
  collect() |>
  View()

r4r:01JMF01XENQWK8XRCF5F2TY91J
r4r:01JMF01YY67T3VPJQ9CH09B8FY


doi:10.1088/1742-6596/2276/1/012021


opencite <- df |>
  group_by(doi) |>
  summarise(count = length(unique(author_id_true)))
fivenum(opencite$count)

eng_phys <- df |>
  group_by(doi) |>
  summarise(count = length(unique(author_id_pred)))
fivenum(eng_phys$count)


foo_rr <- data.frame(
  doi = c("doi_1", "doi_1", "doi_1", "doi_1", "doi_1"),
  da = c("a1", "a2", "a3", "a4", "a5")
)

foo_oc <- data.frame(
  doi = "doi_1",
  da = "oc_1"
)

foo_df <- foo_rr |>
  left_join(foo_oc, by = "doi")


aws.s3::put_object(
  "aap_acp_k_b3/doi_author_pairs.parquet",
  object = "ext_data/doi_author_pairs.parquet",
  bucket = "com-copyright-prodana-shnpc",
  multipart = TRUE
)


r4rid_dob <- dbGetQuery(
  con,
  "SELECT id, degree_of_belief
  FROM distinctAuthor"
)
