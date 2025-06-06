import functions_framework
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
import time
import os
from dotenv import load_dotenv

# Load environment variables (для локального тестування)
load_dotenv()

# Configuration
DATASET = os.getenv('BIGQUERY_DATASET', 'sales')
TABLE = os.getenv('BIGQUERY_TABLE', 'orders')
BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'bkt-sales-data-pj')


@functions_framework.cloud_event
def process_sales_data(cloud_event):
    """
    Triggered by a change in a storage bucket.
    Processes uploaded sales data and loads it into BigQuery.
    """
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]
    bucket = data["bucket"]
    filename = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {filename}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    # Only process certain file types
    if not filename.lower().endswith(('.csv', '.xlsx', '.xls')):
        print(f"Skipping file {filename} - not a supported format")
        return

    try:
        load_to_bigquery(filename, bucket)
        print(f"Successfully processed file: {filename}")
    except Exception as e:
        print(f"Error processing file {filename}: {str(e)}")
        raise


def load_to_bigquery(filename, bucket_name):
    """
    Load data from GCS to BigQuery
    """
    client = bigquery.Client()

    # Create dataset if it doesn't exist
    dataset_id = f"{client.project}.{DATASET}"
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists")
    except:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {dataset_id}")

    # Configure the load job
    table_ref = client.dataset(DATASET).table(TABLE)
    job_config = LoadJobConfig()

    # Determine source format based on file extension
    if filename.lower().endswith('.csv'):
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
    else:
        # For Excel files, you might need additional processing
        print(f"Excel file detected: {filename}")
        # Note: BigQuery doesn't directly support Excel files
        # You might need to convert Excel to CSV first
        return

    job_config.autodetect = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    # Construct the GCS URI
    uri = f'gs://{bucket_name}/{filename}'
    print(f"Loading data from: {uri}")

    # Start the load job
    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config
    )

    # Wait for the job to complete
    load_job.result()

    # Get job statistics
    num_rows = load_job.output_rows
    print(f"Successfully loaded {num_rows} rows into {DATASET}.{TABLE}")

    return num_rows


# For local testing
if __name__ == "__main__":
    # Test the function locally
    test_filename = "test_sales_data.csv"
    test_bucket = BUCKET_NAME

    try:
        result = load_to_bigquery(test_filename, test_bucket)
        print(f"Local test completed: {result} rows loaded")
    except Exception as e:
        print(f"Local test failed: {e}")