import argparse
from apache_beam.options.pipeline_options import PipelineOptions

parser = argparse.ArgumentParser()
print("parser:")
print(parser)
argv = None
parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Input file to process.')
known_args, pipeline_args = parser.parse_known_args(argv)
print("known_args: ")
print(known_args)
print("pipeline_args: ")
print(pipeline_args)
pipeline_options = PipelineOptions(pipeline_args)
print("pipeline_options: ")
print(pipeline_options)
print()
parser = {
    'argv':None,
    'save_main_session':True,
    'file_input': "gs://prueba_myung/input.txt",
    '--runner': "DataflowRunner",
    '--project': "myung-341706",
    '--job_name': "CS_to_BQ",
    '--temp_location': "gs://prueba_myung/temp",
    '--region': "us_central1"
}

for p in parser:
    print(p[0:2] + str(parser[p]))