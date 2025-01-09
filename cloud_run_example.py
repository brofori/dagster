from dagster import job, op, In, Out
import pandas as pd

@op
def load_data() -> pd.DataFrame:
    """Load sample data."""
    data = {
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'city': ['New York', 'San Francisco', 'Seattle']
    }
    return pd.DataFrame(data)

@op
def filter_by_age(df: pd.DataFrame) -> pd.DataFrame:
    """Filter people older than 27."""
    return df[df['age'] > 27]

@op
def format_output(df: pd.DataFrame) -> str:
    """Format the filtered data as a string."""
    return df.to_string()

@job(
    executor_def=cloud_run_executor.configured({
        "project_id": "your-gcp-project-id",
        "region": "us-central1",
        "cpu": 1,
        "memory": "2Gi",
        # Optional: Configure retries
        "retries": {
            "enabled": True,
            "max_retries": 3,
        },
        # Optional: Limit concurrent steps
        "max_concurrent": 2,
        # Optional: Additional Cloud Run job configuration
        "job_template": {
            "labels": {
                "app": "dagster-pipeline"
            }
        },
        "container_template": {
            "env": [
                {"name": "ENVIRONMENT", "value": "production"}
            ]
        }
    })
)
def process_data():
    """A simple ETL pipeline that runs on Cloud Run."""
    data = load_data()
    filtered = filter_by_age(data)
    format_output(filtered)

if __name__ == "__main__":
    # Make sure your Docker image contains all required dependencies
    # and is accessible to Cloud Run
    result = process_data.execute_in_process(
        tags={"docker_image": "your-docker-registry/your-image:tag"}
    ) 