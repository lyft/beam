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


class FlinkKafkaSink(PTransform):
  """
    Custom transform that wraps a Flink Kafka producer - only works with the
    portable Flink runner.

    The translation currently assumes that records are byte arrays
    that represent the Kafka message value.

    There isn't support for Kafka key, timestamp and headers yet.

    Usage example::

      'kafkaSink' >> FlinkKafkaSink().with_topic('beam-example')
        .set_kafka_producer_property('retries', '1000')

    The properties are for the Java Kafka producer. For details see:
    - https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html#kafka-producer
  """
  producer_properties = {'bootstrap.servers': 'localhost:9092'}
  topic = None

  def expand(self, pcoll):
    self._check_pcollection(pcoll)
    return pvalue.PDone(pcoll.pipeline)

  def get_windowing(self, inputs):
    return Windowing(GlobalWindows())

  def infer_output_type(self, unused_input_type):
    return bytes

  def to_runner_api_parameter(self, context):
    assert isinstance(self, FlinkKafkaSink), \
      "expected instance of FlinkKafkaSink, but got %s" % self.__class__
    assert self.topic is not None, "topic not set"
    assert len(self.producer_properties) > 0, "producer properties not set"

    return ("lyft:flinkKafkaSink", json.dumps({
      'topic': self.topic,
      'properties': self.producer_properties}))

  @staticmethod
  @PTransform.register_urn("lyft:flinkKafkaSink", None)
  def from_runner_api_parameter(spec_parameter, _unused_context):
    logging.info("kafka spec: %s", spec_parameter)
    instance = FlinkKafkaInput()
    payload = json.loads(spec_parameter)
    instance.topic = payload['topic']
    instance.producer_properties = payload['properties']
    return instance

  def with_topic(self, topic):
    self.topic = topic
    return self

  def set_kafka_producer_property(self, key, value):
    self.producer_properties[key] = value
    return self

  def with_bootstrap_servers(self, bootstrap_servers):
    return self.set_kafka_consumer_property('bootstrap.servers',
                                            bootstrap_servers)
