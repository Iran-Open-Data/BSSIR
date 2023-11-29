import pandas as pd


def collapse_years(table: pd.DataFrame) -> pd.DataFrame:
    table = table.copy()
    last_year = table.index.to_list()[-1]
    filt = -(table == table.shift(1)).all(axis="columns")
    table = table.loc[filt]
    start_year = table.index.to_series()
    end_year = start_year.shift(-1, fill_value=last_year + 1) - 1
    new_index = start_year.astype(str)
    filt = start_year != end_year
    new_index.loc[filt] = new_index.astype(str) + "-" + end_year.astype(str)
    table.index = pd.Index(new_index, name="Years")
    return table


def maybe_to_numeric(column: pd.Series) -> pd.Series:
    try:
        column = column.str.replace(r"\s+", "", regex=True).astype("Float64")
    except ValueError:
        pass
    try:
        column = column.astype("Int64")
    except ValueError:
        pass
    return column
