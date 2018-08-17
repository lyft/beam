import logging
import json
import sys
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.portability import portable_runner

from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.core import Windowing
from apache_beam import pvalue

from google.protobuf import wrappers_pb2


class FlinkKafkaInput(PTransform):
    """Custom transform that wraps a Flink Kafka consumer - only works with the portable Flink runner."""
    consumer_properties = {'bootstrap.servers' : 'localhost:9092'}
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
        return ("lyft:flinkKafkaInput",
            json.dumps({ 'topic' : self.topic, 'properties' : self.consumer_properties})
                )

    @staticmethod
    @PTransform.register_urn("lyft:flinkKafkaInput", None)
    #@PTransform.register_urn("custom:kafkaInput", wrappers_pb2.BytesValue)
    def from_runner_api_parameter(spec_parameter, unused_context):
        print "spec: " + spec_parameter
        instance = FlinkKafkaInput()
        payload = json.loads(spec_parameter)
        instance.topic = payload['topic']
        instance.consumer_properties = payload['properties']
        return instance

    def with_topic(self, topic):
        self.topic = topic
        return self

    def set_kafka_consumer_property(self, key, value):
        self.consumer_properties[key] = value;
        return self

    def with_bootstrap_servers(self, bootstrap_servers):
        return self.set_kafka_consumer_property('bootstrap.servers', bootstrap_servers)

    def with_group_id(self, group_id):
        return self.set_kafka_consumer_property('group.id', group_id)


if __name__ == "__main__":
  runner = portable_runner.PortableRunner()
  options_string = sys.argv.extend([
      "--experiments=beam_fn_api",
      "--sdk_location=container",
      "--job_endpoint=localhost:8099",
      "--streaming"
  ])
  pipeline_options = PipelineOptions(options_string)

  with beam.Pipeline(runner=runner, options=pipeline_options) as p:
    (p
        #| 'Create' >> beam.Create(['hello', 'world', 'world'])
        #| 'Read' >> ReadFromText("gs://dataflow-samples/shakespeare/kinglear.txt")
        | 'Kafka' >> FlinkKafkaInput().with_topic('beam-example').with_bootstrap_servers('localhost:9092').with_group_id('beam-example-group')
        #| 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        #                .with_output_types(unicode))
        #| 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        #| 'GroupAndSum' >> beam.CombinePerKey(sum)
        | beam.Map(lambda x: logging.info("Got %s", x) or (x, 1)))


