import logging
import sys
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.lyft.kafka import FlinkKafkaInput
from apache_beam.io.lyft.kinesis import FlinkKinesisInput
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.portability import portable_runner


if __name__ == "__main__":
  runner = portable_runner.PortableRunner()
  options_string = sys.argv.extend([
      "--experiments=beam_fn_api",
      "--sdk_location=container",
      "--job_endpoint=localhost:8099",
      "--streaming"
  ])
  pipeline_options = PipelineOptions(options_string)

  # To run with local Kinesalite:
  # docker run -d --name mykinesis -p 4567:4567 instructure/kinesalite
  # AWS_ACCESS_KEY_ID=x; AWS_SECRET_ACCESS_KEY=x
  # aws kinesis create-stream --endpoint-url http://localhost:4567/ --stream-name=beam-example --shard-count=1
  # aws kinesis put-record --endpoint-url http://localhost:4567/ --stream-name beam-example --partition-key 123 --data 'count the words'
  # export AWS_CBOR_DISABLE=1

  with beam.Pipeline(runner=runner, options=pipeline_options) as p:
    (p
        # | 'Create' >> beam.Create(['hello', 'world', 'world'])
        # | 'Read' >> ReadFromText("gs://dataflow-samples/shakespeare/kinglear.txt")
        | 'Kafka' >> FlinkKafkaInput().with_topic('beam-example').with_bootstrap_servers('localhost:9092').with_group_id('beam-example-group')
        # | 'Kinesis' >> FlinkKinesisInput().with_stream('beam-example').with_endpoint('http://localhost:4567', 'fakekey', 'fakesecret')
        #| 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        #                .with_output_types(unicode))
        #| 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        #| 'GroupAndSum' >> beam.CombinePerKey(sum)
        | beam.Map(lambda x: logging.info("Got record: %s", x) or (x, 1)))


