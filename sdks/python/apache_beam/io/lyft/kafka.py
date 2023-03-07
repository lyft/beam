import logging
import json

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.core import Windowing


class FlinkKafkaInput(PTransform):
  """Custom transform that wraps a Flink Kafka consumer - only works with the
  portable Flink runner."""

  def __init__(self):
    self.consumer_properties = dict()
    self.topics = list()
    self.username = None
    self.password = None
    self.max_out_of_orderness_millis = None
    self.start_from_timestamp_millis = None
    self.idleness_timeout_millis = None

  def expand(self, pbegin):
    assert isinstance(pbegin, pvalue.PBegin), (
        'Input to transform must be a PBegin but found %s' % pbegin)
    return pvalue.PCollection(pbegin.pipeline)

  def get_windowing(self, inputs):
    return Windowing(GlobalWindows())

  def infer_output_type(self, unused_input_type):
    return bytes

  def to_runner_api_parameter(self, _unused_context):
    assert isinstance(self, FlinkKafkaInput), \
      "expected instance of FlinkKafkaInput, but got %s" % self.__class__
    assert len(self.topics) > 0, "topics not set"
    assert len(self.consumer_properties) > 0, "consumer properties not set"

    return ("lyft:flinkKafkaInput", json.dumps({
      'topics': self.topics,
      'max_out_of_orderness_millis': self.max_out_of_orderness_millis,
      'start_from_timestamp_millis': self.start_from_timestamp_millis,
      'idleness_timeout_millis': self.idleness_timeout_millis,
      'properties': self.consumer_properties,
      'username': self.username,
      'password': self.password}))

  @staticmethod
  @PTransform.register_urn("lyft:flinkKafkaInput", None)
  def from_runner_api_parameter(_unused_ptransform, spec_parameter, _unused_context):
    logging.info("kafka spec: %s", spec_parameter)
    instance = FlinkKafkaInput()
    payload = json.loads(spec_parameter)
    instance.topics = payload['topics']
    instance.max_out_of_orderness_millis = payload['max_out_of_orderness_millis']
    instance.start_from_timestamp_millis = payload['start_from_timestamp_millis']
    instance.idleness_timeout_millis = payload['idleness_timeout_millis']
    instance.consumer_properties = payload['properties']
    instance.username = payload['username']
    instance.password = payload['password']
    return instance

  def with_topic(self, topic):
    self.topics.append(topic)
    return self

  def set_kafka_consumer_property(self, key, value):
    self.consumer_properties[key] = value
    return self

  def with_bootstrap_servers(self, bootstrap_servers):
    return self.set_kafka_consumer_property('bootstrap.servers',
                                            bootstrap_servers)

  def with_group_id(self, group_id):
    return self.set_kafka_consumer_property('group.id', group_id)

  def with_max_out_of_orderness_millis(self, max_out_of_orderness_millis):
    """
    The interval between the maximum timestamp seen so far and the watermark that
    is emitted. For example, if this is set to 1000ms, after seeing a record for
    10:00:01 we will emit a watermark for 10:00:00, indicating that we believe that all
    data from before that time has arrived.
    """
    self.max_out_of_orderness_millis = max_out_of_orderness_millis
    return self

  def with_start_from_timestamp_millis(self, start_from_timestamp_millis):
    """
    The timestamp to start consuming messages from.
    When using the start timestamp, the pipeline needs to have checkpointing enabled,
    so that on automatic recovery the consumer resumes from the checkpointed offset
    and not the initial timestamp.
    """
    self.start_from_timestamp_millis = start_from_timestamp_millis
    return self

  def with_idleness_timeout_millis(self, idleness_timeout_millis):
    """
    Used in the watermark strategy to generate watermark for idle partitions.
    """
    self.idleness_timeout_millis = idleness_timeout_millis
    return self

  def with_username(self, username):
    """
    The username to consume data from Kafka.
    """
    self.username = username
    return self

  def with_password(self, password):
    """
    The password/credential to consume data from Kafka.
    """
    self.password = password
    return self

@beam.typehints.with_input_types(bytes)
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

  def __init__(self):
    self.producer_properties = dict()
    self.topic = None
    self.username = None
    self.password = None

  def expand(self, pcoll):
    self._check_pcollection(pcoll)
    return pvalue.PDone(pcoll.pipeline)

  def get_windowing(self, inputs):
    return Windowing(GlobalWindows())

  def to_runner_api_parameter(self, _unused_context):
    assert isinstance(self, FlinkKafkaSink), \
      "expected instance of FlinkKafkaSink, but got %s" % self.__class__
    assert self.topic is not None, "topic not set"
    assert len(self.producer_properties) > 0, "producer properties not set"

    return ("lyft:flinkKafkaSink", json.dumps({
      'topic': self.topic,
      'properties': self.producer_properties,
      'username': self.username,
      'password': self.password}))

  @staticmethod
  @PTransform.register_urn("lyft:flinkKafkaSink", None)
  def from_runner_api_parameter(_unused_ptransform, spec_parameter, _unused_context):
    logging.info("kafka spec: %s", spec_parameter)
    instance = FlinkKafkaSink()
    payload = json.loads(spec_parameter)
    instance.topic = payload['topic']
    instance.producer_properties = payload['properties']
    instance.username = payload['username']
    instance.password = payload['password']
    return instance

  def with_topic(self, topic):
    self.topic = topic
    return self

  def set_kafka_producer_property(self, key, value):
    self.producer_properties[key] = value
    return self

  def with_bootstrap_servers(self, bootstrap_servers):
    return self.set_kafka_producer_property('bootstrap.servers',
                                            bootstrap_servers)

  def with_username(self, username):
    """
    The username to write data to Kafka.
    """
    self.username = username
    return self

  def with_password(self, password):
    """
    The password/credential to write data to Kafka.
    """
    self.password = password
    return self