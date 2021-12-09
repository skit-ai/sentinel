"""
sentinel: anomalous call monitoring framework.

Usage:
  sentinel run --config-yml=<config-yml>
  sentinel list filters [--verbose]
  sentinel list exporters

Options:
  --config-yml=<config-yml>      Path to file with sentinel configs.
  --verbose                      Print verbose description of filters.
"""
import io
from docopt import docopt

import yaml
from tabulate import tabulate

import sentinel.util as util
from sentinel.filters.base import FilterFactory
from sentinel.exporters.base import ExporterFactory
from sentinel.dataframes.reader import CSVReader
from sentinel import __version__


def main():
    args = docopt(__doc__, version=__version__)

    if args["run"]:
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
        df_list = filter.handle(df, [])

        exporters = list(config.get("export", {}).keys())

        # Collect exporters from factory to run
        # TODO: make instantiation and chaining more readable

        # Instantiate first exporter
        exporter = ExporterFactory.create_executor(exporters[0], config=config)
        current_exporter = exporter

        # Chain other exporters to each other
        for category in exporters[1:]:
            current_exporter.successor = ExporterFactory.create_executor(category, config=config)
            current_exporter = current_exporter.successor

        df_list = exporter.handle(df_list, filters)

    elif args["list"] and args["filters"]:
        is_verbose = args.get("--verbose")
        headers = ["filters", "description"]
        table_data = []

        for name, factory_item in FilterFactory.registry.items():
            if is_verbose:
                table_data.append((name, factory_item.get('verbose')))
            else:
                table_data.append((name, factory_item.get('description')))

        print("Available filter functions:\n")
        print(tabulate(table_data, headers, tablefmt="grid", colalign=("left",)))

    elif args["list"] and args["exporters"]:
        headers = ["exporters", "description"]
        table_data = []

        for name, factory_item in ExporterFactory.registry.items():
            table_data.append((name, factory_item.get('description')))

        print("Available exporters:\n")
        print(tabulate(table_data, headers, tablefmt="grid", colalign=("left",)))

