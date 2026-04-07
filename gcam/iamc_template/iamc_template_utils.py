import os
from pathlib import Path

import pandas as pd


def get_data_dir(scenario_name: str) -> Path:
    return Path(os.path.join("..", "queries", "queryresults", scenario_name))


def aggregate_with_world(df: pd.DataFrame, group_columns: list[str], world_group_columns: list[str]) -> pd.DataFrame:
    regional = df.groupby(group_columns)["value"].agg("sum").reset_index()
    world = regional.groupby(world_group_columns)["value"].agg("sum").reset_index()
    world["region"] = "World"
    return pd.concat([regional, world], axis=0, ignore_index=True)


def format_for_iamc(
    df: pd.DataFrame,
    scenario_name: str,
    unit: str,
    variable_series: pd.Series,
) -> pd.DataFrame:
    out = df.rename(columns={"region": "Region", "scenario": "Scenario", "Units": "Unit"}).copy()
    out["Scenario"] = scenario_name
    out["Model"] = "GCAM"
    out["Unit"] = unit
    out["Variable"] = variable_series
    return out[["Scenario", "Region", "Model", "Variable", "Unit", "Year", "value"]]


def pivot_iamc(df: pd.DataFrame) -> pd.DataFrame:
    return pd.pivot_table(
        df,
        values=["value"],
        index=["Scenario", "Region", "Model", "Variable", "Unit"],
        columns=["Year"],
        aggfunc="sum",
    ).reset_index()


def calculate_ratio(
    numerator: pd.DataFrame,
    denominator: pd.DataFrame,
    merge_columns: list[str],
) -> pd.DataFrame:
    ratio = pd.merge(
        numerator[merge_columns + ["value"]],
        denominator[merge_columns + ["value"]],
        on=merge_columns,
        suffixes=("_numerator", "_denominator"),
    )
    ratio["value"] = ratio["value_numerator"] / ratio["value_denominator"]
    return ratio[merge_columns + ["value"]]


def combine_and_write(output_frames: list[pd.DataFrame], scenario_name: str, output_filename: str) -> None:
    out_df = pd.concat(output_frames).reset_index(drop=True)
    out_df.columns = ["Scenario", "Region", "Model", "Variable", "Unit"] + [
        str(x[1]) for x in out_df.columns[5:]
    ]

    output_dir = Path(os.path.join("..", "output", scenario_name))
    output_dir.mkdir(parents=True, exist_ok=True)
    out_df.to_excel(output_dir / output_filename, index=False)
