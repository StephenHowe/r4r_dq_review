# Calculate AAP, ACP, K, B3
# Stephen Howe
# 1 June 2025
# Version 1

# versions ----
# v1 20250601 - initial script

# packages ----
library(dplyr)
# library(dbplyr)
library(duckdb)
library(arrow)

# duckDB config ---
con <- dbConnect(duckdb())

# data ----
rr <- open_dataset("aap_acp_k_b3/r4r.parquet")
to_duckdb(rr, con, "r4r") # register r4r data in duckDB
oc <- open_dataset("aap_acp_k_b3/doi_author_pairs.parquet")
to_duckdb(oc, con, "opencite") # register open citations data in duckDB

# Metrics ----
# Filter datasets to just those DOIs in RR
df <- dbGetQuery(
  con,
  "SELECT
    r4r.doi,
    r4r.author_id AS author_id_pred,
    opencite.author_id AS author_id_true
  FROM r4r
  JOIN opencite ON r4r.doi = opencite.doi"
)

df_test <- read.csv("aap_acp_k_b3/test_true_data.csv")

# Average Author Purity (AAP) ----
aap <- df_test |>
  filter(author_id_true == "author_id_4") |>
  group_by(author_id_pred) |>
  summarise(
    total = n(),
    majority_count = max(table(author_id_true))
  ) |>
  summarise(AAP = mean(majority_count / total)) |>
  pull(AAP)



# ChatGPT test

calculate_aap <- function(df) {
  # Total number of records
  N <- nrow(df)

  # Step 1: Compute n_ij = count of records in each true/predicted cluster pair
  n_ij <- df %>%
    count(author_id_true, author_id_pred, name = "n_ij")

  # Step 2: Compute n_j = size of each true cluster
  n_j <- df %>%
    count(author_id_true, name = "n_j")

  # Step 3: Join to associate n_j with each n_ij
  n_ij <- n_ij %>%
    left_join(n_j, by = "author_id_true")

  # Step 4: Apply the formula
  aap <- sum((n_ij$n_ij^2) / n_ij$n_j) / N

  return(aap)
}

calculate_aap(df_test)

df_test |>
  filter(author_id_true == "author_id_1") |>
  calculate_aap()

df |> calculate_aap()


calculate_acp <- function(df) {
  N <- nrow(df)

  # Count n_ij: number of items in predicted cluster i and true cluster j
  n_ij <- df %>%
    count(author_id_pred, author_id_true, name = "n_ij")

  # Count n_i: total number of items in predicted cluster i
  n_i <- df %>%
    count(author_id_pred, name = "n_i")

  # Merge and compute ACP terms
  acp_df <- n_ij %>%
    left_join(n_i, by = "author_id_pred") %>%
    mutate(term = (n_ij^2) / n_i)

  # Final ACP calculation
  acp <- sum(acp_df$term) / N
  return(acp)
}

df_test |> calculate_acp()
df_test |>
  filter(author_id_pred == "da_C" |
           author_id_pred == "da_B" |
           author_id_pred == "da_I" ) |>
  calculate_acp()
