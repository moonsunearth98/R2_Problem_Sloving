Hotel Entity Resolution Pipeline
PySpark • Databricks • Distributed Processing

Overview:

This project implements a scalable entity resolution pipeline to identify duplicate hotel listings across multiple suppliers.
In real-world travel platforms, the same hotel can appear multiple times because different suppliers provide their own listings with slightly different:

hotel names
address formats
capitalization
abbreviations

This pipeline processes supplier datasets and groups such records into a single unified hotel entity using distributed data processing and fuzzy string matching.
The solution is built using Apache Spark on Databricks Serverless Compute, allowing it to scale efficiently for large datasets.

Problem Statement:

Multiple suppliers may provide listings for the same hotel but with small differences.

Example:

Supplier	Hotel Name	Address
supplier1	Grand Plaza Hotel	123 Main St
supplier2	Grand Plaza	123 Main Street
supplier3	Grand Plaza Hotel NYC	123 Main St.

Although these refer to the same hotel, they appear as separate records.

The goal of this pipeline is to:

✔ Identify such duplicates
✔ Group them into a single hotel entity
✔ Produce a unified mapping table

Architecture

The pipeline follows a distributed processing workflow.

Raw Supplier Data (CSV)
        │
        ▼
Data Cleaning & Standardization
        │
        ▼
Coordinate Validation
        │
        ▼
Geographic Bucketing
        │
        ▼
Candidate Pair Generation
        │
        ▼
Fuzzy Matching (Name + Address)
        │
        ▼
Hotel Entity Mapping
        │
        ▼
Final Mapping Output

Technology Stack (The pipeline was executed using): 

Databricks	Development 
Compute	Serverless Compute
Language	Python (PySpark)
Storage	DBFS

Input dataset location:

/FileStore/tables/suppliers_data.csv
Data Processing Pipeline

1. Data Ingestion:

The supplier dataset is read from DBFS using Spark's distributed CSV reader.

Key columns used:

inv_id
supplier
name
address
city
latitude
longitude

2. Data Standardization

To improve matching accuracy, text columns are normalized.

Operations performed:

Convert hotel names to lowercase
Convert addresses to lowercase
Convert city names to lowercase
This reduces inconsistencies caused by formatting differences.

3. Coordinate Validation:

Some rows contain invalid coordinates (for example country names instead of numbers).

Example invalid record:

latitude = "SOUTH KOREA"

Such rows are removed to ensure geographic calculations remain valid.

4. Geographic Bucketing

To avoid comparing every hotel with every other hotel, a geographic blocking technique is used.

Hotels are grouped using:

geo_bucket = floor(latitude * 10) + "_" + floor(longitude * 10)

This groups hotels that are geographically close.

Benefits:
Reduces comparison space
Improves performance
Makes pipeline scalable

5. Candidate Pair Generation

Hotels are compared only if they share:

the same city
the same geo_bucket
This significantly reduces unnecessary comparisons.

Instead of:

N² comparisons
I only compare locally relevant candidates.

6. Fuzzy Matching

Fuzzy string matching is used to measure similarity between hotel records.

Two similarity metrics are calculated:
Name Similarity
Calculated using Levenshtein Distance between hotel names.
Address Similarity
Calculated using Levenshtein Distance between addresses.

Both scores are normalized and combined:

final_score = 0.7 * name_score + 0.3 * address_score

Hotel names receive higher weight because they are stronger identifiers.

7. Duplicate Detection

A similarity threshold determines whether two hotels represent the same entity.
MATCH_THRESHOLD = 75
Pairs exceeding this threshold are classified as duplicates.

8. Entity Mapping

Duplicate hotels are grouped under a single HQ hotel identifier (hq_id).

Final output columns:

Column	  Description
hq_id    	Master hotel identifier
inv_id	  Supplier hotel ID
inv_name	Supplier name

Example Output
hq_id	inv_id	inv_name
1001	1001	supplier1
1001	4550	supplier2
1001	8781	supplier3

This indicates that multiple supplier records correspond to the same hotel entity.

Output Location:

The final mapping dataset is written to:

/FileStore/hotel_supplier_mapping

The output is exported as a single CSV file for submission.

Performance Optimizations:

The pipeline incorporates several performance improvements:
Geographic blocking to reduce comparisons
Spark distributed processing
Repartitioning for parallel execution
Avoidance of expensive cross joins

These optimizations ensure the solution remains efficient for large datasets.
Verification
A validation step was included to check whether any hq_id maps to multiple supplier records.
In this dataset, most records did not exceed the similarity threshold, which indicates that supplier listings were largely distinct.
As a result, many hotels map to themselves in the final output.

Result:

The pipeline successfully:

✔ Cleans and validates supplier hotel data
✔ Identifies potential duplicate hotels
✔ Applies fuzzy matching to detect similar records
✔ Groups records into unified hotel entities
✔ Produces a scalable mapping dataset
