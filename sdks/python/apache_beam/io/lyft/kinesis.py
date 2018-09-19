import json
import logging

from apache_beam import pvalue
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.core import Windowing


class FlinkKinesisInput(PTransform):
  """Custom transform that wraps a Flink Kinesis consumer - only works with the
  portable Flink runner."""
  consumer_properties = {
    'aws.region': 'us-east-1',
    'flink.stream.initpos': 'TRIM_HORIZON'
  }
  stream = None

  def expand(self, pbegin):
    assert isinstance(pbegin, pvalue.PBegin), (
        'Input to transform must be a PBegin but found %s' % pbegin)
    return pvalue.PCollection(pbegin.pipeline)

  def get_windowing(self, inputs):
    return Windowing(GlobalWindows())

  def infer_output_type(self, unused_input_type):
    return bytes

  def to_runner_api_parameter(self, context):
    assert isinstance(self, FlinkKinesisInput), \
      "expected instance of FlinkKinesisInput, but got %s" % self.__class__
    assert self.stream is not None, "topic not set"
    assert len(self.consumer_properties) > 0, "consumer properties not set"

    return ("lyft:flinkKinesisInput", json.dumps({
      'stream': self.stream,
      'properties': self.consumer_properties}))

  @staticmethod
  @PTransform.register_urn("lyft:flinkKinesisInput", None)
  def from_runner_api_parameter(spec_parameter, _unused_context):
    logging.info("kinesis spec: %s", spec_parameter)
    instance = FlinkKinesisInput()
    payload = json.loads(spec_parameter)
    instance.stream = payload['stream']
    instance.consumer_properties = payload['properties']
    return instance

  def with_stream(self, stream):
    self.stream = stream
    return self

  def set_consumer_property(self, key, value):
    self.consumer_properties[key] = value
    return self

  def with_endpoint(self, endpoint, access_key, secret_key):
    # cannot have both region and endpoint
    self.consumer_properties.pop('aws.region', None)
    self.set_consumer_property('aws.endpoint', endpoint)
    self.set_consumer_property('aws.credentials.provider.basic.accesskeyid', access_key)
    self.set_consumer_property('aws.credentials.provider.basic.secretkey', secret_key)
    return self
