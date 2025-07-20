# Freetrade – Louis Wain

##  Overview

This pipeline retrieves data from FakerAPI, converts it to a parquet file type and uploads it to GCS. You can run this pipeline by by running the `pipeline/main.py` file.

---

## Libraries Used

- `requests` – for handling the HTTP API call
- `pyarrow` – for converting JSON to Parquet format in memory
- `google-cloud-storage` – for uploading files to GCS
- `io.BytesIO` – to avoid local disk writes

---

## Pipeline Steps

1. **Extract JSON from API**  
   Sends a GET request to the API and extracts the data in JSON format.

2. **Transform to Parquet**  
   Convert the JSON payload (list of dicts) to an Apache Arrow table and write it to an in-memory buffer using `pyarrow`.

3. **Upload to GCS**  
   Stream the buffer contents directly to a parquet file in the GCS bucket.

