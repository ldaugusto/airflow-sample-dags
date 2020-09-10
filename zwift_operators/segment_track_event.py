#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow.models import BaseOperator
from airflow.providers.segment.hooks.segment import SegmentHook
from airflow.utils.decorators import apply_defaults
import csv
import gzip


class SegmentTrackEventOperator(BaseOperator):
    """
    Send Track Event to Segment for a specified user_id and event

    :param csv_file: a CSV (plain or zipped) path with data to be used as event properties.
        One of them *must* be called 'userId' (templated)
    :type csv_file: str
    :param event: The name of the event you're tracking. (templated)
    :type event: str
    :param segment_conn_id: The connection ID to use when connecting to Segment.
    :type segment_conn_id: str
    :param segment_debug_mode: Determines whether Segment should run in debug mode.
        Defaults to False
    :type segment_debug_mode: bool
    """
    template_fields = ('csv_file', 'event')
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(self,
                 event,
                 csv_file,
                 segment_conn_id='segment_default',
                 segment_debug_mode=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.event = event
        self.segment_debug_mode = segment_debug_mode
        self.segment_conn_id = segment_conn_id
        self.csv_file = csv_file
        self.hook = SegmentHook(segment_conn_id=self.segment_conn_id,
                                segment_debug_mode=self.segment_debug_mode)
        self.analytics = self.hook.get_conn()

    def execute(self, context):

        if self.csv_file.endswith('.gz'):
            file_reader = gzip.open(self.csv_file, "rt", newline="")
        else:
            file_reader = open(self.csv_file, 'r')

        csv_reader = csv.DictReader(file_reader)
        for row in csv_reader:
            # converts a csv row into a map: header1 -> value, header2 -> value...
            props = dict(row)
            user_id = props.pop('userId', None)
            for key in props:
                props[key] = retype(props.pop(key))

            if user_id is None:
                self.log.info('No userId set in CSV row: %s >>> Skipping.', props)
                continue

            self.log.info('Sending track event (%s) for userId %s with properties: %s',
                          self.event, user_id, props)

            self.analytics.track(user_id=user_id, event=self.event, properties=props)


def retype(value=''):
    if value.isnumeric():
        try:
            return int(value)
        except ValueError:
            return float(value)
    else:
        return value
