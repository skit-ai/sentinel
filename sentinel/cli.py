"""
sentinel

Usage:
  sentinel lookout --config-yml=<config-yml>
  sentinel list

Options:
  --config-yml=<config-yml>      Path to file with sentinel configs.
"""
import io
from docopt import docopt

import yaml

import sentinel.util as util
from sentinel.analyses.base import AnalysisFactory
from sentinel.exporters.slack import SlackExporter
from sentinel.dataframes.reader import CSVReader
from sentinel import __version__


def main():
    args = docopt(__doc__, version=__version__)

    if args["lookout"]:
        config_file = args["--config-yml"]
        config = yaml.load(open(config_file, "r"), Loader=yaml.FullLoader)

        # Download dataframe and get pandas.DataFrame
        s3_obj = util.download_file(config.get("data_url"))
        csv_reader = CSVReader(io.BytesIO(s3_obj['Body'].read()))
        df = csv_reader.read()

        # Get set of filters to run
        filters = list(config.get("filters", {}).keys())

        if not filters:
            return None

        # Collect filters from factory to run
        filter = AnalysisFactory.create_executor(filters[0])
        for category in filters[1:]:
            filter.successor = AnalysisFactory.create_executor(category)

        # Start execution of analysis functions
        df = filter.handle(df)

        export_medium = config.get("export", {})
        if export_medium.get("slack"):
            slack_exporter = SlackExporter()
            slack_exporter.export_report(df, filters)

    elif args["list"]:
        print("Available analysis functions:\n")
        for name, factory_item in AnalysisFactory.registry.items():
            print(f"{name}: {factory_item.get('description')}")
