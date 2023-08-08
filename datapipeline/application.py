import argparse
from os import path, pardir

from etl.ETLPipeline import ETLPipeline
from etl.datasources.ufodatasource import UFOSightingDataSource

from webscraper.ufosightingwebscraper import UFOSightingWebScraper

from etl.extract_strategies import (
    CSVExtractStrategy,
    ExcelExtractStrategy,
    WebScraperExtractStrategy,
)
from etl.load_strategies import (
    DataLoadStrategy,
    CSVLoadStrategy,
    ExcelLoadStrategy,
    SQLiteLoadStrategy,
    JSONLoadStrategy
)


class Application:
    def __init__(self, args: list[str]) -> None:
        self.program_context = self.__get_program_context(self.__parse_args(args))
        self.input_dir = path.join(pardir, "data", "in")
        self.output_dir = path.join(pardir, "data", "out")


    def __parse_args(self, args: list[str]):
        parser = argparse.ArgumentParser(
            description=
            """
            This program aggregates data from multiple sources into a target
            repository for analysis. Output to csv, excel, json, or a sqlite db.
            """
        )

        parser.add_argument(
            "outfile", type=str, help="Name of the output file."
        )
        parser.add_argument(
            "-f", "--format",
            type=str,
            choices=["csv", "xlsx", "json", "sqlite"],
            default="xlsx",
            help="Output location for the aggregated data. xlsx by default.",
        )

        return parser.parse_args(args)


    def __get_program_context(self, args: argparse.Namespace):
        load_strategy_lookup: dict[str, DataLoadStrategy] = {
            "csv": CSVLoadStrategy,
            "xlsx": ExcelLoadStrategy,
            "json": JSONLoadStrategy,
            "sqlite": SQLiteLoadStrategy
        }

        return f"{args.outfile}.{args.format}", load_strategy_lookup[args.format]


    def run(self):
        """
        Set up source/destination repositories and run data pipeline
        """
        outfile, target_repo = self.program_context
        destination = target_repo(path.join(self.output_dir, outfile))

        ufo_data_2020 = UFOSightingDataSource(
            CSVExtractStrategy(path.join(self.input_dir, "sightings-2020.csv")),
            destination
        )
        ufo_data_2021 = UFOSightingDataSource(
            ExcelExtractStrategy(path.join(self.input_dir, "sightings-2021.xlsx")),
            destination
        )
        ufo_data_2022 = UFOSightingDataSource(
            WebScraperExtractStrategy(
                UFOSightingWebScraper('https://nuforc.org/webreports')
            ),
            destination
        )
        ufo_data_2023 = UFOSightingDataSource(
            CSVExtractStrategy(path.join(self.input_dir, "sightings-2023.csv")),
            destination
        )

        pipeline = ETLPipeline([
            ufo_data_2020,
            ufo_data_2021,
            ufo_data_2022,
            ufo_data_2023
        ])
        pipeline.run()
