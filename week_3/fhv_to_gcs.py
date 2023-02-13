from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    #    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    #    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    #path = Path(f"data/{dataset_file}.parquet")
    path = Path(f"data/fhv/{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip")
    #df.to_parquet(path, compression="gzip")
    return path


@flow()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(log_prints=True)
def etl_web_to_gcs(months: list[int] = [2, 3], year: int = 2019) -> None:
    """The main ETL function""" 
    count_rows = 0
    for month in months:
        dataset_file = f"fhv_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
        df = fetch(dataset_url)
        df_clean = clean(df) 
               
        count_rows += len(df.index)
        
        path = write_local(df_clean, dataset_file)
        write_gcs(path)
    print(f'{count_rows} rows was written to Google cloud storage')

if __name__ == "__main__":
    months = [2,3,4,5,6,7,8,9,10,11,12,1] 
    year = 2019
    etl_web_to_gcs(months, year)

