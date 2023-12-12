from typing import Iterable

import pandas as pd
import yaml

from ..api import API
from . import datadoc_utils


def create_otnt_part(table_name: str, years: list[int], api: API) -> pd.DataFrame:
    availability_dir = api.defaults.docs.csv.joinpath("availability")
    availability_table = (
        pd.read_csv(availability_dir.joinpath(f"{table_name}.csv"), index_col=0)
        .fillna("")
        .map(lambda x: x.upper())
        .replace("", None)
    )

    if len(years) == 0:
        return pd.DataFrame()
    rows = []
    for year in years:
        table_meta = api.utils.resolve_metadata(api.metadata.tables[table_name], year)
        all_columns = table_meta["columns"]
        if all_columns is None:
            continue
        row = availability_table.loc[year].dropna()
        mapped_row = (
            row.map(all_columns)
            .fillna("drop")
            .map(lambda x: "drop" if (x == "drop") else x["new_name"])
        )
        rows.append(pd.Series(mapped_row.to_list(), index=row))
    if len(rows) == 0:
        return pd.DataFrame()
    old_to_new = pd.DataFrame(rows, index=years).fillna("")
    columns = old_to_new.columns.to_list()
    columns.sort()
    old_to_new = old_to_new.loc[:, columns]
    return old_to_new


def create_old_to_new_tables(
    table_name: str, api: API
) -> tuple[pd.DataFrame, pd.DataFrame]:
    years = [year for _, year in api.utils.create_table_year_pairs(table_name, "all")]

    if table_name == "house_specifications":
        year_parts = [
            [year for year in years if year in range(1363, 1383)],
            [year for year in years if year >= 1383],
        ]
    else:
        year_parts = [
            [year for year in years if year in range(1363, 1384)],
            [year for year in years if year >= 1384],
        ]

    return tuple(
        create_otnt_part(table_name, year_part, api) for year_part in year_parts
    )


def create_new_to_old_table(table_name: str, api: API) -> pd.DataFrame:
    availability_dir = api.defaults.docs.csv.joinpath("availability")
    availability_table = (
        pd.read_csv(availability_dir.joinpath(f"{table_name}.csv"), index_col=0)
        .fillna("")
        .map(lambda x: x.upper())
        .replace("", None)
    )
    years = [year for _, year in api.utils.create_table_year_pairs(table_name, "all")]
    rows = []
    for year in years:
        all_columns = api.utils.resolve_metadata(
            api.metadata.tables[table_name]["columns"], year
        )
        row = availability_table.loc[year].replace("", None).dropna()
        mapped_row = (
            row.map(all_columns)
            .fillna("drop")
            .map(lambda x: x if x == "drop" else x["new_name"])
        )
        filt = mapped_row != "drop"
        row = row.loc[filt]
        mapped_row = mapped_row.loc[filt]
        rows.append(pd.Series(row.to_list(), index=mapped_row))
    new_to_old = pd.DataFrame(rows, index=pd.Index(years, name="Years"))
    new_to_old = (
        new_to_old.transpose()
        .sort_values(new_to_old.index.to_list()[-1])
        .transpose()
        .fillna("")
    )
    return new_to_old


def remove_next_lines(dictionary: dict) -> dict:
    for key in dictionary:
        if isinstance(dictionary[key], str):
            dictionary[key] = dictionary[key].replace("\n", " ").replace("  ", " ")
        elif isinstance(dictionary[key], dict):
            dictionary[key] = remove_next_lines(dictionary[key])
        elif isinstance(dictionary[key], (int, float)):
            pass
        elif isinstance(dictionary[key], list):
            dictionary[key] = [
                element.replace("\n", " ").replace("  ", " ")
                for element in dictionary[key]
            ]
    return dictionary


def generate_columns_metadata(
    api: API, table_names: str | Iterable[str] | None = None
) -> None:
    table_names = [table_names] if isinstance(table_names, str) else table_names
    table_names = (
        list(api.metadata.tables["table_availability"].keys())
        if table_names is None
        else table_names
    )
    for table_name in table_names:
        new_to_old = create_new_to_old_table(table_name, api)
        for column in new_to_old.columns:
            directory = api.defaults.docs.csv.joinpath("cleaned", table_name, column)
            directory.mkdir(exist_ok=True, parents=True)
            col_meta = api.utils.extract_column_metadata(column, table_name)
            col_meta = remove_next_lines(col_meta)
            with directory.joinpath("metadata.yaml").open(
                mode="w", encoding="utf-8"
            ) as file:
                yaml.safe_dump(
                    col_meta,
                    file,
                    sort_keys=False,
                    encoding="utf-8",
                    allow_unicode=True,
                )


def create_replace_tables(
    table_name: str, api: API, raw_tables: dict[int, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    new_to_old = create_new_to_old_table(table_name, api)
    years = api.utils.parse_years("all", table_name=table_name)
    replace_tables = {}
    for column_name in new_to_old.columns:
        old_name_dict = new_to_old[column_name].fillna("").to_dict()
        replace_table_dict = {}
        for year in years:
            table_meta = api.utils.resolve_metadata(api.metadata.tables, year)
            assert isinstance(table_meta, dict)
            columns_meta = table_meta[table_name]["columns"]
            if old_name_dict[year] == "":
                continue
            old_col_name = old_name_dict[year]
            column_metadata = columns_meta[old_col_name]
            table = raw_tables[year]
            table.columns = table.columns.str.upper()
            replace_dict = (
                column_metadata["replace"]
                if (
                    ("replace" in column_metadata)
                    and (column_metadata["replace"] is not None)
                )
                else {}
            )
            if len(replace_dict) == 0:
                continue
            annual_table = pd.DataFrame()
            for key, value in replace_dict.items():
                annual_table.at[key, "Replace_Value"] = str(value)
                annual_table.at[key, "Frequency"] = table[old_col_name].eq(key).sum()
            replace_table_dict[year] = annual_table.loc[annual_table["Frequency"] > 0]

        if len(replace_table_dict) == 0:
            continue
        replace_table = pd.concat(replace_table_dict, names=["Year", "Value"])
        replace_table = replace_table.round(2).reset_index()
        if replace_table.empty:
            continue
        replace_tables[column_name] = replace_table
    return replace_tables


def generate_replace_tables(
    api: API, table_names: str | Iterable[str] | None = None
) -> None:
    table_names = [table_names] if isinstance(table_names, str) else table_names
    table_names = (
        list(api.metadata.tables["table_availability"].keys())
        if table_names is None
        else table_names
    )
    for table_name in table_names:
        raw_tables = {
            year: api.load_table(
                table_name,
                year,
                form="raw",
            )
            for year in api.utils.parse_years("all", table_name=table_name)
        }
        replace_tables = create_replace_tables(
            table_name=table_name, api=api, raw_tables=raw_tables
        )
        for column_name, replace_table in replace_tables.items():
            directory = api.defaults.docs.csv.joinpath(
                "cleaned", table_name, column_name
            )
            directory.mkdir(exist_ok=True, parents=True)
            replace_table.to_csv(directory.joinpath("replace_table.csv"))


def create_summary_stats(
    table_name: str, api: API, tables: dict[int, pd.DataFrame]
) -> dict[str, dict[str, pd.DataFrame]]:
    new_to_old = create_new_to_old_table(table_name, api)

    summary_tables = {}
    for column_name in new_to_old.columns:
        old_name_dict = new_to_old[column_name].fillna("").to_dict()
        summary_tables[column_name] = {}
        for year, table in tables.items():
            if column_name not in table.columns:
                continue
            table_meta = api.utils.resolve_metadata(
                api.metadata.tables[table_name], year
            )
            assert isinstance(table_meta, dict)
            if "type" not in table_meta["columns"][old_name_dict[year]]:
                print(year, column_name, old_name_dict[year])
            col_type = table_meta["columns"][old_name_dict[year]]["type"]
            if col_type == "string":
                if "string" not in summary_tables[column_name]:
                    summary_tables[column_name]["string"] = []
                summary_tables[column_name]["string"].append(
                    table[column_name].value_counts(dropna=False).rename(year)
                )
            elif col_type == "category":
                if "category" not in summary_tables[column_name]:
                    summary_tables[column_name]["category"] = []
                summary_tables[column_name]["category"].append(
                    table[column_name].value_counts(dropna=False).rename(year)
                )
            elif col_type == "boolean":
                if "boolean" not in summary_tables[column_name]:
                    summary_tables[column_name]["boolean"] = []
                summary_tables[column_name]["boolean"].append(
                    table[column_name]
                    .apply(
                        {
                            "True": lambda s: s.sum(),
                            "False": lambda s: s.eq(False).sum(),
                            "Missing": lambda s: s.isna().sum(),
                        }
                    )
                    .rename(year)
                )
            else:
                if "numeric" not in summary_tables[column_name]:
                    summary_tables[column_name]["numeric"] = []
                summary_tables[column_name]["numeric"].append(
                    table[column_name]
                    .apply(
                        {
                            "Count": "count",
                            "Mean": "mean",
                            "Standard Deviation": "std",
                            "Minimum": "min",
                            "Median": "median",
                            "Maximum": "max",
                        }
                    )
                    .rename(year)
                )
        for col_type, s_list in summary_tables[column_name].items():
            table = pd.DataFrame(s_list).rename_axis("Year", axis="index")
            if col_type in ["category", "string", "boolean"]:
                table = (
                    table.loc[
                        :, table.iloc[-1].sort_values(ascending=False).index.to_list()
                    ]
                    .div(table.sum(axis="columns"), axis="index")
                    .mul(100)
                    .round(2)
                )
            else:
                table = table.round(2)
            summary_tables[column_name][col_type] = table
    return summary_tables


def generate_summary_stats(
    api: API, table_names: str | Iterable[str] | None = None
) -> None:
    table_names = [table_names] if isinstance(table_names, str) else table_names
    table_names = (
        list(api.metadata.tables["table_availability"].keys())
        if table_names is None
        else table_names
    )
    for table_name in table_names:
        cleaned_tables = {
            year: api.load_table(table_name, year, form="cleaned", on_missing="create")
            for year in api.utils.parse_years("all", table_name=table_name)
        }
        summary_stats = create_summary_stats(table_name, api, cleaned_tables)
        for column, tables in summary_stats.items():
            directory = api.defaults.docs.csv.joinpath("cleaned", table_name, column)
            directory.mkdir(exist_ok=True, parents=True)
            for data_type, table in tables.items():
                table.to_csv(directory.joinpath(f"{data_type}.csv"))


def create_category_table(table_name: str, column_name: str, api: API):
    old_col_dict = create_new_to_old_table(table_name, api)
    category_years = {}
    for year in api.utils.parse_years("all", table_name=table_name):
        old_name = old_col_dict[column_name][year]
        if old_name == "":
            continue
        column_metadata = api.utils.resolve_metadata(
            api.metadata.tables[table_name]["columns"], year
        )[old_name]
        if "categories" not in column_metadata:
            continue
        category_years[year] = column_metadata["categories"]
    table = (
        pd.Series(category_years)
        .apply(pd.Series)
        .fillna("")
        .pipe(datadoc_utils.collapse_years)
        .transpose()
    )
    try:
        table = table.sort_index()
    except TypeError:
        table.index = table.index.astype(str)
        table = table.sort_index()

    return table


def create_cleaned_table_page(table_name: str, api: API) -> str:
    years = api.utils.parse_years("all", table_name=table_name)
    cleaned_tables = {
        year: api.load_table(table_name, year, form="cleaned", on_missing="create")
        for year in years
    }

    md_page_content = ""
    md_page_content += f"# {table_name}\n\n"

    md_page_content += "## Old to New Titles\n\n"
    otn_p1, otn_p2 = create_old_to_new_tables(table_name, api)
    if not otn_p1.empty:
        md_page_content += datadoc_utils.collapse_years(otn_p1).to_markdown()
        md_page_content += "\n\n\n"
    if not otn_p2.empty:
        md_page_content += datadoc_utils.collapse_years(otn_p2).to_markdown()
        md_page_content += "\n\n\n"

    md_page_content += "## New to Old Titles\n\n"
    nto = create_new_to_old_table(table_name, api)
    md_page_content += datadoc_utils.collapse_years(nto).to_markdown()
    md_page_content += "\n\n\n"

    summary_dict = create_summary_stats(table_name, api, cleaned_tables)
    md_page_content += "## Columns Details\n\n"

    for column in nto.columns:
        directory = api.defaults.docs.csv.joinpath("cleaned", table_name, column)

        md_page_content += f"### {column}\n\n"

        with directory.joinpath("metadata.yaml").open(encoding="utf-8") as file:
            metadata = file.read()
        metadata = (
            "    ``` yaml\n    " + metadata.replace("\n", "\n    ") + "\n    ```\n"
        )
        md_page_content += '??? abstract "Column Metadata"\n'
        md_page_content += metadata

        md_page_content += "#### Column Codes\n\n"
        column_code_table = datadoc_utils.collapse_years(nto[[column]])
        filt = column_code_table[column] != ""
        column_code_table.loc[filt, column] = (
            "["
            + column_code_table.loc[filt, column]
            + f"](/{api.defaults.package_name}/tables/raw/{table_name}#"
            + column_code_table.loc[filt, column].str.lower()
            + ")"
        )
        md_page_content += column_code_table.to_markdown()
        md_page_content += "\n\n\n"

        if column in summary_dict:
            md_page_content += "#### Summary Statistics\n\n"
            for dtype, sum_table in summary_dict[column].items():
                md_page_content += f"**{dtype} data**\n\n"
                md_page_content += sum_table.fillna("").to_markdown()
                md_page_content += "\n\n\n"
                if dtype == "category":
                    md_page_content += "#### Categories\n\n"
                    md_page_content += create_category_table(
                        table_name, column, api
                    ).to_markdown()
                    md_page_content += "\n\n\n"

        if directory.joinpath("replace_table.csv").exists():
            md_page_content += "#### Replacements\n\n"
            replace_table = pd.read_csv(
                directory.joinpath("replace_table.csv"), index_col=0
            )
            md_page_content += replace_table.to_markdown(
                index=False,
            )
            md_page_content += "\n\n\n"

    return md_page_content


def generate_cleaned_description(
    api: API, table_names: str | Iterable[str] | None = None
) -> None:
    table_names = [table_names] if isinstance(table_names, str) else table_names
    table_names = (
        list(api.metadata.tables["table_availability"].keys())
        if table_names is None
        else table_names
    )
    for table_name in table_names:
        md_page_content = create_cleaned_table_page(table_name, api)
        md_file_path = api.defaults.docs.cleaned_tables.joinpath(f"{table_name}.md")
        with md_file_path.open(mode="w", encoding="utf-8") as md_file:
            md_file.write(md_page_content)
