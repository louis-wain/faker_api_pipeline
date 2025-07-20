import requests
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import io

class FakerAPIPipeline:
    def __init__(self):
        self.api_url = "https://fakerapi.it/api/v1/users?_quantity=100"
        self.bucket_name = "freetrade-data-eng-hiring"
        self.file_name = "louis_wain/data_engineering_task.parquet"
        self.client = storage.Client.create_anonymous_client()
        self.bucket = self.client.bucket(self.bucket_name)

    def extract_data(self):
        """Sends a GET request to Faker API

        Pulls 100 rows from FakerAPI, as defined by the self.api_url variable.

        Returns:
            Python list containing rows of data pulled from the API
        """
        r = requests.get(self.api_url)
        if r.status_code == 200:
            data = r.json()
            data = data['data']
            return data
        else:
            raise requests.HTTPError(f"Failed with status code: {r.status_code}")

        
    def upload_to_gcs(self, data: list):
        """Uploads the data to specified GCS bucket

        Converts the data into a parquet file in memory then uploads to the
        specified GCS bucket.

        Args:
            data: Python list containing data to be uploaded.

        """
        try:
            # Write parquet using pyarrow
            table = pa.Table.from_pylist(data)
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)

            # Upload to gcs
            blob = self.bucket.blob(self.file_name)
            blob.upload_from_file(buffer, rewind=True)

        except pa.ArrowInvalid as e:
            raise ValueError(f"Invalid Arrow table data: {e}")
        except pa.ArrowIOError as e:
            raise IOError(f"Failed to write to parquet: {e}")
        except Exception as e:
            raise RuntimeError(f"Upload to bucket failed: {e}")
    

    def run_pipeline(self):
        """Runs the pipeline

        Runs the extract_data function, followed by the upload_to_gcs function 
        to run the pipeline.
        """
        data = self.extract_data()
        self.upload_to_gcs(data)
        

        
pipe = FakerAPIPipeline()
pipe.run_pipeline()