import pandas as pd
from pandas.api.types import infer_dtype
import yaml

from ..api import API

from . import datadoc_utils


def create_raw_summary_table(table: pd.DataFrame) -> pd.DataFrame:
    table = clean_raw_data(table)
    rows = len(table.index)
    description = table.isna().sum().to_frame("Missing_Count")
    description["Availability_Ratio"] = (
        description["Missing_Count"].div(rows).sub(1).mul(-100)
    )
    description["Data_Type"] = table.apply(infer_dtype)
    description["Unique_Values"] = table.apply(lambda s: len(s.unique()))
    description["Frequent_Values"] = table.apply(
        lambda s: "; ".join(
            [
                f"{key}: {value:,}"
                for key, value in s.value_counts().head(3).to_dict().items()
            ]
        )
    )
    description.index.name = "Column"
    return description


def clean_raw_data(table: pd.DataFrame) -> pd.DataFrame:
    table = table.replace(r"^\s*$", None, regex=True)
    for col in table.columns:
        if table[col].dtype == "object":
            table[col] = table[col].str.strip()
            table[col] = datadoc_utils.maybe_to_numeric(table[col])
        elif table[col].dtype == "float64":
            try:
                table[col] = table[col].astype("Int64")
            except (ValueError, TypeError):
                pass
    return table


def generate_availability_tables(api: API):
    availability_dir = api.defaults.docs.csv.joinpath("availability")
    availability_dir.mkdir(exist_ok=True, parents=True)
    for table_name in api.metadata.tables["table_availability"]:
        years = [
            year for _, year in api.utils.create_table_year_pairs(table_name, "all")
        ]
        columns = []
        for year in years:
            columns.append(
                api.load_table(table_name, year, form="raw").columns.to_list()
            )
        availability = pd.DataFrame(columns, index=pd.Index(years, name="Year"))

        availability.to_csv(availability_dir.joinpath(f"{table_name}.csv"))


def generate_raw_summary_tables(api: API):
    for table_name in api.metadata.tables["table_availability"]:
        years = [
            year for _, year in api.utils.create_table_year_pairs(table_name, "all")
        ]
        for year in years:
            table = api.load_table(table_name, year, form="raw")
            summary_table = create_raw_summary_table(table)
            raw_table_dir = api.defaults.docs.csv.joinpath("raw", table_name)
            raw_table_dir.mkdir(exist_ok=True, parents=True)
            summary_table.to_csv(raw_table_dir.joinpath(f"{year}.csv"))


def file_code_table(table_name: str, api: API) -> pd.DataFrame:
    years = [year for _, year in api.utils.create_table_year_pairs(table_name, "all")]

    table = (
        pd.Series(
            {
                year: api.utils.resolve_metadata(
                    api.metadata.tables[table_name]["file_code"], year
                )
                for year in years
            }
        )
        .str.replace("*", "\\*", regex=False)
        .to_frame(name="File Code")
        .rename_axis("Year", axis="index")
        .pipe(datadoc_utils.collapse_years)
    )

    return table


def create_column_code_summary_tables(api: API) -> dict[str, pd.DataFrame]:
    availability_dir = api.defaults.docs.csv.joinpath("availability")
    column_code_summary_tables = {}
    for table_name in api.metadata.tables["table_availability"]:
        column_names = (
            pd.read_csv(availability_dir.joinpath(f"{table_name}.csv"), index_col=0)
            .unstack()
            .dropna()
            .drop_duplicates()
            .sort_values()
            .to_list()
        )
        years = [
            year for _, year in api.utils.create_table_year_pairs(table_name, "all")
        ]
        annual_summary_tables = {}
        for year in years:
            csv_path = api.defaults.docs.csv.joinpath("raw", table_name, f"{year}.csv")
            annual_summary_tables[year] = pd.read_csv(csv_path, index_col=0).fillna("")

        for column_name in column_names:
            column_list = []
            for year in years:
                try:
                    column_list.append(
                        annual_summary_tables[year].loc[column_name].rename(year)
                    )
                except KeyError:
                    pass
            column_code_summary_tables[column_name.upper()] = pd.DataFrame(column_list)
    return column_code_summary_tables


def remove_next_lines(dictionary: dict) -> dict:
    if not isinstance(dictionary, dict):
        return dictionary
    for key in dictionary:
        if isinstance(dictionary[key], str):
            dictionary[key].strip().strip("\n")
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


def generate_raw_description(api: API):
    for table_name in api.metadata.tables["table_availability"]:
        md_page_content = ""
        md_page_content += f"# {table_name}\n\n"

        md_page_content += "## Table Code\n\n"
        md_page_content += file_code_table(table_name, api).to_markdown()
        md_page_content += "\n\n\n"

        years = [
            year for _, year in api.utils.create_table_year_pairs(table_name, "all")
        ]

        md_page_content += "## Columns Availability\n\n"
        csv_path = api.defaults.docs.csv.joinpath("availability", f"{table_name}.csv")
        availability = (
            pd.read_csv(csv_path, index_col=0)
            .fillna("")
            .pipe(datadoc_utils.collapse_years)
        )
        md_page_content += availability.to_markdown()
        md_page_content += "\n\n\n"

        md_page_content += "## Annual Summary Tables\n\n"
        annual_summary_tables = {}
        for year in years:
            csv_path = api.defaults.docs.csv.joinpath("raw", table_name, f"{year}.csv")
            annual_summary_tables[year] = pd.read_csv(csv_path, index_col=0).fillna("")
        for year in years:
            md_page_content += f"### {year}\n\n"
            summary_table = annual_summary_tables[year]
            summary_table.columns = summary_table.columns.str.replace("_", " ")
            md_page_content += summary_table.to_markdown()
            md_page_content += "\n\n\n"

        md_page_content += "## Column Code Summary Tables\n\n"
        column_code_summary_tables = create_column_code_summary_tables(api)
        for column_name, table in column_code_summary_tables.items():
            md_page_content += f"### {column_name}\n\n"

            metadata = yaml.safe_dump(
                api.utils.exteract_code_metadata(column_name, table_name),
                sort_keys=False,
                encoding="utf-8",
                allow_unicode=True,
            )
            metadata = remove_next_lines(metadata.decode())
            metadata = (
                "    ``` yaml\n    " + metadata.replace("\n", "\n    ") + "\n    ```\n"
            )
            md_page_content += '??? abstract "Column Metadata"\n'
            md_page_content += metadata

            md_page_content += table.to_markdown()
            md_page_content += "\n\n\n"

        md_file_path = api.defaults.docs.raw_tables.joinpath(f"{table_name}.md")
        with md_file_path.open(mode="w", encoding="utf-8") as md_file:
            md_file.write(md_page_content)
