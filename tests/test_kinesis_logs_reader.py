#  Copyright 2016 Observable Networks
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import unicode_literals
from datetime import datetime
# from io import StringIO
from json import dumps
from unittest import TestCase

try:
    from unittest.mock import MagicMock, patch
except ImportError:
    from mock import MagicMock, patch

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from kinesis_logs_reader import KinesisLogsReader
from kinesis_logs_reader.__main__ import main as cli_main
from kinesis_logs_reader.utils import gunzip_bytes, gzip_bytes


def _data_message(log_events):
    data = {'messageType': 'DATA_MESSAGE', 'logEvents': []}
    for event in log_events:
        data['logEvents'].append({'extractedFields': event})

    return {'Data': gzip_bytes(dumps(data).encode('utf-8'))}


def _control_message():
    data = {'messageType': 'CONTROL_MESSAGE'}
    return {'Data': gzip_bytes(dumps(data).encode('utf-8'))}


def _create_event(index):
    return {
        'srcaddr': '192.0.2.1',
        'srcport': index,
        'dstaddr': '198.51.100.1',
        'dstport': 443,
        'protocol': 6,
    }


def _get_shard_iterator(**kwargs):
    return {'ShardIterator': '{}_iterator-0001'.format(kwargs['ShardId'])}


DESCRIBE_STREAM = {
    'StreamDescription': {
        'Shards': [{'ShardId': 'shard-0001'}, {'ShardId': 'shard-0002'}],
    }
}


GET_RECORDS = {
    'shard-0001_iterator-0001': {
        'Records': [
            _control_message(),
            _data_message([_create_event(0), _create_event(1)]),
        ],
        'NextShardIterator': 'shard-0001_iterator-0002',
        'MillisBehindLatest': 100,
    },
    'shard-0001_iterator-0002': {
        'Records': [_data_message([_create_event(2), _create_event(3)])],
        'NextShardIterator': 'shard-0001_iterator-0003',
        'MillisBehindLatest': 0,
    },
    'shard-0002_iterator-0001': {
        'Records': [_data_message([_create_event(4), _create_event(5)])],
        'NextShardIterator': 'shard-0002_iterator-0002',
        'MillisBehindLatest': 0,
    },
    'shard-0002_iterator-0002': {
        'Records': [],
        'NextShardIterator': 'shard-0002_iterator-0003',
        'MillisBehindLatest': 0,
    },
}


def _get_client(*args, **kwargs):
    # Mock the boto3 client so tests don't hit the AWS API
    mock_client = MagicMock()
    mock_client.get_paginator.return_value.paginate.return_value = (
        [DESCRIBE_STREAM]
    )
    mock_client.get_shard_iterator.side_effect = _get_shard_iterator
    mock_client.get_records.side_effect = (
        lambda **kwargs: GET_RECORDS[kwargs['ShardIterator']]
    )

    return mock_client


class UtilsTestCase(TestCase):
    def setUp(self):
        self.data = b'Test data'
        self.gz_data = (
            b'\x1f\x8b\x08\x00M\x986W\x02\xff\x0bI-.QHI,I\x04\x00\x11,\xf9Q\t'
            b'\x00\x00\x00'
        )

    def test_gunzip_bytes(self):
        self.assertEqual(gunzip_bytes(self.gz_data), self.data)

    def test_gzip_bytes(self):
        gz_data = gzip_bytes(self.data)
        self.assertEqual(gunzip_bytes(gz_data), self.data)


class KinesisLogsReaderTestCase(TestCase):
    def __init__(self, *args, **kwargs):
        # Python 2 compatibility for tests
        if not hasattr(self, 'assertCountEqual'):
            self.assertCountEqual = self.assertItemsEqual

        return super(KinesisLogsReaderTestCase, self).__init__(*args, **kwargs)

    def setUp(self):
        self.stream_name = 'test-stream'
        self.start_time = datetime(2016, 5, 13, 22, 55, 0)

        self.reader = KinesisLogsReader(
            self.stream_name, kinesis_client=_get_client()
        )

    def test_init(self):
        kwargs = {'region_name': 'test-region', 'profile_name': 'test_profile'}
        patch_path = 'kinesis_logs_reader.kinesis_logs_reader.Session'
        with patch(patch_path) as mock_Session:
            KinesisLogsReader(self.stream_name, **kwargs)
            mock_Session.assert_called_once_with(**kwargs)
            mock_Session.return_value.client.assert_called_once_with('kinesis')

    def test_get_shard_ids(self):
        actual = list(self.reader._get_shard_ids())
        expected = ['shard-0001', 'shard-0002']
        self.assertCountEqual(actual, expected)

    def test_get_shard_iterators(self):
        shard_id = 'shard-0001'
        actual = self.reader._get_shard_iterator(shard_id)
        self.assertEqual(actual, 'shard-0001_iterator-0001')
        self.reader.kinesis_client.get_shard_iterator.assert_called_with(
            StreamName=self.stream_name,
            ShardId=shard_id,
            ShardIteratorType='LATEST',
        )

        shard_id = 'shard-0002'
        actual = self.reader._get_shard_iterator(shard_id, self.start_time)
        self.assertEqual(actual, 'shard-0002_iterator-0001')
        self.reader.kinesis_client.get_shard_iterator.assert_called_with(
            StreamName=self.stream_name,
            ShardId=shard_id,
            ShardIteratorType='AT_TIMESTAMP',
            Timestamp=self.start_time,
        )

    def test_read_shard(self):
        shard_id = 'shard-0001'
        self.reader.shard_iterators[shard_id] = 'shard-0001_iterator-0001'

        # Data messages should have been extracted and returned
        actual_results = list(self.reader._read_shard(shard_id))
        expected_results = [_create_event(0), _create_event(1)]
        self.assertEqual(actual_results, expected_results)

        # The iterator should have advanced
        actual_iterator = self.reader.shard_iterators[shard_id]
        expected_iterator = 'shard-0001_iterator-0002'
        self.assertEqual(actual_iterator, expected_iterator)

        # The response showed the stream as "behind"
        self.assertFalse(self.reader.shard_finished[shard_id])

    def test_reader(self):
        actual = list(self.reader)
        expected = [_create_event(x) for x in range(6)]
        self.assertCountEqual(actual, expected)


@patch(
    'kinesis_logs_reader.kinesis_logs_reader.KinesisLogsReader._get_client',
    _get_client
)
class MainTestCase(TestCase):
    def test_cli_main(self):
        argv = [
            '--start-time="2016-05-15 01:02"',
            '--time-format="%Y-%m-%d %H:%M"',
            'test_stream',
        ]
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            cli_main(argv)
            output = mock_stdout.getvalue().splitlines()

        self.assertEqual(len(output), 7)

        event = _create_event(0)
        event_keys = sorted(event.keys())
        event_values = [event[k] for k in event_keys]

        actual_header = output[0]
        expected_header = '\t'.join(event_keys)
        self.assertEqual(actual_header, expected_header)

        actual_row = output[1]
        expected_row = '\t'.join(str(x) for x in event_values)
        self.assertEqual(actual_row, expected_row)

    def test_cli_count(self):
        argv = ['--count=2', 'test_stream']
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            cli_main(argv)
            output = mock_stdout.getvalue().splitlines()

        self.assertEqual(len(output), 3)
