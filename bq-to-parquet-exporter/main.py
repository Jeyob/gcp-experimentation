from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
import pyarrow.parquet as pq
from pyarrow import fs
from datetime import timedelta

from google.cloud import storage
import google.auth


def generate_download_signed_url_v4(bucket_name):
    credentials, project_id = google.auth.default()
    print('credentials: ', credentials)
    print('project_id: ', project_id)

    storage_client = storage.Client()
    bucket = storage_client.bucket(
        bucket_name=bucket_name, user_project='main-383907')
    url = bucket.generate_signed_url(
        credentials=credentials,
        version='v4',
        expiration=timedelta(minutes=15),
        method='GET',
    )

    print('Generated GET signed URL: ')
    print(url)


def main():
    client = BigQueryReadClient()
    requested_session = types.ReadSession()
    requested_session.table = "projects/{}/datasets/{}/tables/{}".format(
        "main-383907", "bq_basketball", "mbb_players_cpy"
    )
    requested_session.data_format = types.DataFormat.ARROW
    requested_session.read_options.selected_fields = ['season', 'gametime']
    requested_session.read_options.row_restriction = 'season in (2017, 2014)'

    parent = 'projects/main-383907'
    session = client.create_read_session(
        parent=parent,
        read_session=requested_session,
        max_stream_count=1,
    )

    reader = client.read_rows(session.streams[0].name)

    rows = reader.rows(session)
    table = rows.to_arrow()

    gcs = fs.GcsFileSystem(retry_time_limit=timedelta(seconds=15))

    pq.write_to_dataset(
        table=table,
        root_path='bq_experiments/mbb_players',
        partition_cols=['season'],
        existing_data_behavior='delete_matching',
        filesystem=gcs
    )

    generate_download_signed_url_v4('bq_experiments/mbb_players')


if __name__ == '__main__':
    main()
