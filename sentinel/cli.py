"""
sentinel: anomalous call monitoring framework.

Usage:
  sentinel lookout --config-yml=<config-yml>
  sentinel list

Options:
  --config-yml=<config-yml>      Path to file with sentinel configs.
"""
from docopt import docopt

import yaml
from tabulate import tabulate

from sentinel.filters.base import FilterFactory
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
        # TODO: make instantiation and chaining more readable

        # Instantiate first filter
        filter = FilterFactory.create_executor(
            filters[0], **config["filters"][filters[0]].get("kwargs", {}))
        current_filter = filter

        # Chain other filters to each other
        for category in filters[1:]:
            current_filter.successor = FilterFactory.create_executor(
                category, **config["filters"][category].get("kwargs", {}))
            current_filter = current_filter.successor

        # Start execution of analysis functions
        df = filter.handle(df)

        export_medium = config.get("export", {})
        if export_medium.get("slack"):
            slack_exporter = SlackExporter()
            slack_exporter.export_report(df, filters)

    elif args["list"]:
        headers = ["filters", "description"]
        table_data = []
        for name, factory_item in FilterFactory.registry.items():
            table_data.append((name, factory_item.get('description')))

        print("Available filter functions:\n")
        print(tabulate(table_data, headers, tablefmt="pretty", colalign=("left",)))
