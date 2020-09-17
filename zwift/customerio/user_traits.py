from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from customerio import CustomerIO
import csv
import gzip


class CustomerIoIdentifyOperator(BaseOperator):
    """
    Send Track Event to Segment for a specified user_id and event

    :param csv_file: a CSV (plain or zipped) path with data to be used as event properties.
        One of them *must* be called 'userId' (templated)
    :type csv_file: str
    :param customerio_site_id: The 'site_id' property from Customer.IO account we want to update
    :type customerio_site_id: str
    :param customerio_api_key: The 'api_key' property from Customer.IO account we want to update
    :type customerio_api_key: str
    """
    template_fields = ('csv_file', 'event')
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(self,
                 csv_file,
                 customerio_site_id,
                 customerio_api_key,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.csv_file = csv_file
        self.cio = CustomerIO(site_id=customerio_site_id, api_key=customerio_api_key)

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

            if user_id is None:
                self.log.info('No userId set in CSV row: %s >>> Skipping.', props)
                continue

            # fixing numerics types set as strings from csv
            clean_props = dict()
            for key in props:
                clean_props[key] = retype(props.get(key))

            self.log.info('Sending identify for userId %s with properties: %s', user_id, clean_props)

            self.cio.identify(id=user_id, **clean_props)


def retype(value=''):
    if value.isnumeric():
        try:
            return int(value)
        except ValueError:
            return float(value)
    else:
        return value
