import json
import logging

from apache_beam import PTransform
from apache_beam.pvalue import PBegin
from apache_beam.pvalue import PCollection
from apache_beam.transforms.core import Windowing
from apache_beam.transforms.window import GlobalWindows


class S3AndKinesisInput(PTransform):
    """Custom composite transform that uses Kinesis and S3 as
    input sources. This wraps the streamingplatform-dryft-sdk SourceConnector.
    Only works with the portable Flink runner.
    """

    def expand(self, pbegin):
        assert isinstance(pbegin, PBegin), (
                'Input to transform must be a PBegin but found %s' % pbegin)
        return PCollection(pbegin.pipeline)

    def get_windowing(self, inputs):
        return Windowing(GlobalWindows())

    def with_program_config(self, program_config_path):
        self.program_config_path = program_config_path
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
            "expected instact of S3AndKinesisInput, but got %s" % self.__class__
        assert self.program_config_path is not None, "Program config file path not set"

        return ("lyft:flinkS3AndKinesisInput", json.dumps({
            'program_config_path': self.program_config_path
        }))
