# 🎬 Spark Movie Data Analysis

A scalable **Movie Data Analysis** project built with **Apache Spark / PySpark** and **Spark SQL**. The project ingests Movies, Ratings, and Tags datasets from HDFS, transforms them into Spark DataFrames, performs analytical SQL queries, and writes the resulting datasets back to HDFS as CSV outputs.

---

## 📌 Project Overview

This project demonstrates an end-to-end Big Data analytics workflow using Apache Spark on a GCP-based environment.

The analysis works with three core datasets:

- **Movies** — movie ID, title, and genres
- **Ratings** — user ID, movie ID, rating, and timestamp
- **Tags** — user ID, movie ID, tag, and timestamp

The workflow includes data ingestion, schema definition, timestamp conversion, Spark SQL analysis, aggregation, joins, ranking/window functions, and HDFS-based output generation.

---

## 🏗️ Architecture / Workflow

```text
                    ┌──────────────────────┐
                    │   Movies CSV         │
                    └──────────┬───────────┘
                               │
                    ┌──────────▼───────────┐
                    │                      │
                    │      HDFS            │
                    │   Data Storage       │
                    │                      │
                    └──────────┬───────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
          ▼                    ▼                    ▼
   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
   │ Movies DF    │      │ Ratings DF  │      │  Tags DF    │
   └──────┬──────┘      └──────┬──────┘      └──────┬──────┘
          │                    │                    │
          └────────────────────┼────────────────────┘
                               ▼
                    ┌──────────────────────┐
                    │   Spark SQL / CTEs   │
                    │ Joins & Aggregations │
                    │ Window Functions     │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Analytical Outputs   │
                    │        CSV           │
                    └──────────┬───────────┘
                               ▼
                    ┌──────────────────────┐
                    │        HDFS          │
                    │   Output Directory   │
                    └──────────────────────┘
```

---

## 🛠️ Tech Stack

| Category | Technology |
|---|---|
| Programming Language | **Python 3** |
| Big Data Processing | **Apache Spark / PySpark** |
| Query Engine | **Spark SQL** |
| Cloud Platform | **Google Cloud Platform (GCP)** |
| GCP Services | **Dataproc, Google Cloud Storage (GCS)** |
| Distributed Storage | **HDFS** |
| SQL / Data Warehouse Technology | **Apache Hive** |
| Development Environment | **Microsoft VS Code** |
| Notebook Environment | **JupyterLab** |
| Input Format | **CSV** |
| Output Format | **CSV** |

---

## 🔄 Data Engineering Pipeline

### 1. Data Ingestion

The Movies, Ratings, and Tags CSV files are placed in HDFS and loaded into Spark DataFrames.

```python
df_movies = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/tmp/spark_movie/movies.csv")
```

Ratings and Tags use explicitly defined Spark schemas before loading the data.

### 2. Data Transformation

Unix timestamps in the Ratings and Tags datasets are converted into Spark `TimestampType` values.

```python
df_ratings = df_ratings.withColumn(
    "timestamp",
    from_unixtime("timestamp").cast(TimestampType())
)
```

### 3. Spark SQL Layer

The three DataFrames are registered as temporary SQL views:

```python
df_movies.createOrReplaceTempView("MOVIES")
df_ratings.createOrReplaceTempView("RATINGS")
df_tags.createOrReplaceTempView("TAGS")
```

Spark SQL is then used for aggregations, joins, CTEs, ranking, and analytical queries.

### 4. Output Generation

Analytical results are written back to HDFS as CSV files. The project uses `coalesce(1)` to generate a single output file for each analysis and writes results using overwrite mode.

---

## 📊 Analysis Performed

The project generates multiple analytical outputs, including:

1. **Aggregated number of ratings per year**
2. **Average monthly rating**
3. **Rating-level distribution**
4. **Movies tagged but not rated**
5. **Movies rated but not tagged**
6. **Top movies based on average rating and rating count**
7. **Tags per movie vs. tags per user**
8. **Users who tagged but did not rate movies**
9. **Ratings per user vs. ratings per movie**
10. **Predominant genre per rating level**
11. **Predominant tag per genre**
12. **Top 10 popular movies based on distinct users**
13. **Top 10 movies with more than 30 users reviewing them**

---

## 📁 Generated Output Files

The analytical outputs are saved under the Spark movie output directory in HDFS.

| # | Output File | Analysis |
|---:|---|---|
| 1 | `agg_Ratings.csv` | Number of ratings aggregated by year |
| 2 | `avg_monthly_Ratings.csv` | Average rating by month |
| 3 | `distribution_ratings.csv` | Rating-level distribution and percentage |
| 4 | `tagged_not_rated.csv` | Movies tagged but not rated |
| 5 | `rated_not_tagged.csv` | Movies rated but not tagged |
| 6 | `top_10_avgratings&count_ratings.csv` | Top-ranked movies by average rating and rating count |
| 7 | `tags_per_movieVStags_per_user.csv` | Tags per movie compared with tags per user |
| 8 | `users_tagged_not_rate.csv` | Users who tagged but did not rate |
| 9 | `ratings_per_userVSratings_per_movie.csv` | Ratings per user compared with ratings per movie |
| 10 | `freq_genre_per_rating.csv` | Most frequent genre for each rating level |
| 11 | `freq_tag_per_genre.csv` | Most frequent tag for each genre |
| 12 | `popular_movies.csv` | Top 10 movies by distinct users who rated them |
| 13 | `top_10_morethan30users.csv` | Top 10 movies with more than 30 distinct reviewers |

---

## 🔍 Key Spark SQL Techniques

The project demonstrates practical Spark SQL techniques such as:

- `GROUP BY`
- `ORDER BY`
- `COUNT`
- `COUNT(DISTINCT ...)`
- `AVG`
- `ROUND`
- `CASE WHEN`
- `LEFT JOIN`
- `INNER JOIN`
- Common Table Expressions (**CTEs / WITH**)
- `DENSE_RANK()`
- Window functions
- Date/timestamp extraction
- Temporary SQL views
- Filtering with `HAVING`
- Data aggregation and comparison

---

## ☁️ GCP & Distributed Processing

The assignment documentation specifies an Apache Spark environment running on a **GCP cluster**, with **Dataproc/GCS** services, JupyterLab, and Apache Hive.

The documented processing flow is:

```text
CSV Files
   ↓
HDFS
   ↓
Spark DataFrames
   ↓
Spark SQL
   ↓
Analytical Results
   ↓
HDFS Output
```

The HDFS output directory is also checked through the HDFS Namenode UI after the analytical files are generated.

---

## 📂 Project Structure

```text
Spark-Movie-Data-Analysis/
│
├── Spark_MovieRating.ipynb
├── Spark_Ass2.pdf
├── README.md
│
└── output/
    ├── agg_Ratings.csv
    ├── avg_monthly_Ratings.csv
    ├── distribution_ratings.csv
    ├── tagged_not_rated.csv
    ├── rated_not_tagged.csv
    ├── top_10_avgratings&count_ratings.csv
    ├── tags_per_movieVStags_per_user.csv
    ├── users_tagged_not_rate.csv
    ├── ratings_per_userVSratings_per_movie.csv
    ├── freq_genre_per_rating.csv
    ├── freq_tag_per_genre.csv
    ├── popular_movies.csv
    └── top_10_morethan30users.csv
```

---

## 🚀 How to Run

### Prerequisites

Make sure the environment has:

- Python 3
- Apache Spark / PySpark
- Access to a Spark cluster
- HDFS
- JupyterLab or VS Code
- Apache Hive / Hive support where required

### Run the Notebook

Open:

```text
Spark_MovieRating.ipynb
```

Start a Spark session with Hive support:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark with Hive") \
    .enableHiveSupport() \
    .getOrCreate()
```

Then execute the notebook cells sequentially.

---

## 💾 HDFS Locations

### Input

```text
/tmp/spark_movie/movies.csv
/tmp/spark_movie/ratings.csv
/tmp/spark_movie/tags.csv
```

### Output

```text
/tmp/output_data/spark_movie/
```

---

## 📈 Project Highlights

- Built a complete **distributed movie analytics pipeline** using PySpark.
- Processed **Movies, Ratings, and Tags** datasets using Spark DataFrames.
- Implemented explicit schemas for structured data ingestion.
- Converted Unix timestamps into Spark timestamps.
- Used **Spark SQL** for analytical querying.
- Applied SQL joins, CTEs, aggregations, and window functions.
- Generated multiple business-oriented movie analytics.
- Stored analytical results in **HDFS as CSV outputs**.
- Used `coalesce(1)` for single-file output generation.
- Demonstrated Spark execution in a **GCP cluster environment**.

---

## 🎯 Learning Outcomes

This project demonstrates practical experience with:

- Big Data processing
- Distributed data ingestion
- PySpark DataFrame operations
- Spark SQL analytics
- HDFS storage
- GCP-based Spark environments
- Data aggregation
- SQL joins and CTEs
- Window functions and ranking
- Timestamp transformation
- Batch analytical processing
- Structured CSV output generation

---

## 👨‍💻 Project Files

| File | Description |
|---|---|
| `Spark_MovieRating.ipynb` | PySpark notebook containing the complete movie data analysis |
| `Spark_Ass2.pdf` | Assignment documentation and processing workflow |
| `README.md` | Project documentation |

---

## ⭐ Project Summary

**Spark Movie Data Analysis** is a Big Data analytics project that uses **PySpark, Spark SQL, HDFS, and GCP Spark infrastructure** to transform raw movie-related datasets into structured analytical outputs. It demonstrates an end-to-end workflow from distributed data ingestion through SQL-based analysis and HDFS output generation.
