import pyarrow as pa
import pyarrow.dataset as ds

from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1.reader import ReadRowsIterable

from pydantic import BaseModel

from typing import Literal
from datetime import timedelta
from pyarrow import fs

from time import perf_counter


class Configuration(BaseModel):
    project: str
    dataset: str
    table: str
    format: int  # 0 - unspecified, 1 - Avro, 2 - Arrow
    selected_field: list[str]
    row_restriction: str
    partition_cols: list[str]
    destination: Literal['local', 'gcs']
    destination_dir: str


client = BigQueryReadClient()


def get_stream_reader(config: Configuration):
    client = BigQueryReadClient()
    requested_session = types.ReadSession()
    requested_session.table = "projects/{}/datasets/{}/tables/{}".format(
        config.project, config.dataset, config.table
    )
    requested_session.data_format = config.format
    requested_session.read_options.selected_fields = config.selected_field
    requested_session.read_options.row_restriction = config.row_restriction

    parent = f'projects/{config.project}'
    session = client.create_read_session(
        parent=parent,
        read_session=requested_session,
        max_stream_count=1,
    )

    reader = client.read_rows(session.streams[0].name)

    schema = pa.ipc.read_schema(pa.py_buffer(
        session.arrow_schema.serialized_schema))

    rows = reader.rows(session)

    return schema, rows


def write_data(schema: pa.Schema, rows: ReadRowsIterable, config: Configuration):
    def _generator(rows: ReadRowsIterable):
        for page in rows.pages:
            yield page.to_arrow()

    destination_fs = None if config.destination == 'local' else fs.GcsFileSystem(
        retry_time_limit=timedelta(seconds=15))

    t1_start = perf_counter()

    ds.write_dataset(
        data=_generator(rows=rows),
        base_dir=config.destination_dir,
        format='parquet',
        partitioning=config.partition_cols,
        partitioning_flavor='hive',
        use_threads=True,
        existing_data_behavior='delete_matching',
        create_dir=True,
        schema=schema,
        filesystem=destination_fs,
        min_rows_per_group=100000,
    )

    t1_stop = perf_counter()
    print('Elasped time: {:.2f}s'.format(t1_stop-t1_start))


def main():
    conf = Configuration(
        project='main-383907',
        dataset='bq_basketball',
        table='taxi_trips',
        format=types.DataFormat.ARROW,
        selected_field=[],
        # 'season in (2017, 2014)',
        row_restriction="cast(timestamp_trunc(trip_start_timestamp, day) as date) between date_sub('2017-04-17', interval 10 day) and date('2017-04-17')",
        partition_cols=['company'],
        destination='local',
        destination_dir='bq_experiments/taxi_trips_export'
    )

    # Get reader
    schema, rows = get_stream_reader(conf)

    # write stream
    write_data(schema, rows, conf)


if __name__ == '__main__':
    main()
