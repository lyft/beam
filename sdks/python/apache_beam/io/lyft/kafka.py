import logging
import json

from apache_beam import pvalue
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.core import Windowing


class FlinkKafkaInput(PTransform):
  """Custom transform that wraps a Flink Kafka consumer - only works with the
  portable Flink runner."""
  consumer_properties = {'bootstrap.servers': 'localhost:9092'}
  topic = None

  def expand(self, pbegin):
    assert isinstance(pbegin, pvalue.PBegin), (
        'Input to transform must be a PBegin but found %s' % pbegin)
    return pvalue.PCollection(pbegin.pipeline)

  def get_windowing(self, inputs):
    return Windowing(GlobalWindows())

  def infer_output_type(self, unused_input_type):
    return bytes

  def to_runner_api_parameter(self, context):
    assert isinstance(self, FlinkKafkaInput), \
      "expected instance of CustomKafkaInput, but got %s" % self.__class__
    assert self.topic is not None, "topic not set"
    assert len(self.consumer_properties) > 0, "consumer properties not set"

    return ("lyft:flinkKafkaInput", json.dumps({
      'topic': self.topic,
      'properties': self.consumer_properties}))

  @staticmethod
  @PTransform.register_urn("lyft:flinkKafkaInput", None)
  def from_runner_api_parameter(spec_parameter, _unused_context):
    logging.info("kafka spec: %s", spec_parameter)
    instance = FlinkKafkaInput()
    payload = json.loads(spec_parameter)
    instance.topic = payload['topic']
    instance.consumer_properties = payload['properties']
    return instance

  def with_topic(self, topic):
    self.topic = topic
    return self

  def set_kafka_consumer_property(self, key, value):
    self.consumer_properties[key] = value
    return self

  def with_bootstrap_servers(self, bootstrap_servers):
    return self.set_kafka_consumer_property('bootstrap.servers',
                                            bootstrap_servers)

  def with_group_id(self, group_id):
    return self.set_kafka_consumer_property('group.id', group_id)
