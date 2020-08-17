import json
import logging
import random
import string
from collections import namedtuple

from apache_beam import PTransform
from apache_beam.pvalue import PBegin
from apache_beam.pvalue import PCollection
from apache_beam.transforms.core import Windowing
from apache_beam.transforms.window import GlobalWindows

Event = namedtuple('Event', 'name lateness_in_sec lookback_days')
KinesisConfig = namedtuple('KinesisConfig', 'name properties parallelism stream_start_mode')
S3Config = namedtuple('S3Config', 'parallelism lookback_hours')


class S3AndKinesisInput(PTransform):
    """Custom composite transform that uses Kinesis and S3 as
    input sources. This wraps the streamingplatform-dryft-sdk SourceConnector.
    Only works with the portable Flink runner.
    """

    def __init__(self):
        super().__init__()
        self.events = []
        self.s3_config = S3Config(None, None)
        self.source_name = 'S3andKinesis_' + self._get_random_source_name()

    def expand(self, pbegin):
        assert isinstance(pbegin, PBegin), (
                'Input to transform must be a PBegin but found %s' % pbegin)
        return PCollection(pbegin.pipeline)

    def get_windowing(self, inputs):
        return Windowing(GlobalWindows())

    def with_event(self, event):
        self.events.append(event)
        return self

    def with_kinesis_config(self, kinesis_config):
        self.kinesis_config = kinesis_config
        return self

    def with_s3_config(self, s3_config):
        self.s3_config = s3_config
        return self

    def with_source_name(self, source_name):
        self.source_name = source_name
        return self

    @staticmethod
    @PTransform.register_urn("lyft:flinkS3AndKinesisInput", None)
    def from_runner_api_parameter(_unused_ptransform, spec_parameter, _unused_context):
        logging.info("S3AndKinesis spec :" + spec_parameter)
        instance = S3AndKinesisInput()
        payload = json.loads(spec_parameter)
        instance.program_config_path = payload['program_config_path']
        return instance

    def to_runner_api_parameter(self, _unused_context):
        assert isinstance(self, S3AndKinesisInput), \
            "expected instance of S3AndKinesisInput, but got %s" % self.__class__
        assert self.kinesis_config is not None, "Kinesis config not set"
        assert isinstance(self.kinesis_config, KinesisConfig), \
            "expected instance of KinesisConfig, but got %s" % type(self.kinesis_config)

        json_map = {
            'source_name': self.source_name,
            'kinesis': {
                'stream': self.kinesis_config.name,
                'properties': self.kinesis_config.properties,
                'parallelism': self.kinesis_config.parallelism,
                'stream_start_mode': self.kinesis_config.stream_start_mode
            },
            's3': {
                'parallelism': self.s3_config.parallelism,
                'lookback_hours': self.s3_config.lookback_hours
            },
        }

        event_list_json = []
        for e in self.events:
            assert isinstance(e, Event), "expected instance of Event, but got %s" % type(e)
            event_map = {'name': e.name, 'lateness_in_sec': e.lateness_in_sec, 'lookback_days': e.lookback_days}
            event_list_json.append(event_map)

        json_map['events'] = event_list_json

        return "lyft:flinkS3AndKinesisInput", json.dumps(json_map)

    def _get_random_source_name(self):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for _ in range(4))
