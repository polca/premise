import argparse
from pathlib import Path

import pandas as pd

from iamc_template_registry import CREATOR_FUNCTIONS


def sort_strings_then_numbers(item):
	is_number = str(item).isdigit()
	return (is_number, str(item))


def get_output_dir(scenario_name: str) -> Path:
	return Path("..") / "output" / scenario_name


def build_combined_workbook(scenario_name: str) -> None:
	output_dir = get_output_dir(scenario_name)
	excel_files = sorted(
		path
		for path in output_dir.glob("*.xlsx")
		if scenario_name not in path.stem and path.name != f"gcam_{scenario_name}.xlsx"
	)
	dataframes = [pd.read_excel(path) for path in excel_files]
	out_df = pd.concat(dataframes, axis=0)
	out_df = out_df.reindex(sorted(out_df.columns, key=sort_strings_then_numbers), axis=1)
	out_df.to_excel(output_dir / f"gcam_{scenario_name}.xlsx", index=False)


def run_scenario(scenario_name: str) -> None:
	for creator in CREATOR_FUNCTIONS:
		creator(scenario_name)
	build_combined_workbook(scenario_name)


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Generate IAMC template workbooks from GCAM query outputs.")
	parser.add_argument(
		"scenarios",
		nargs="*",
		default=["SSP2"],
		help="Scenario names under ../queries/queryresults to process.",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	for scenario_name in args.scenarios:
		run_scenario(scenario_name)


if __name__ == "__main__":
	main()
