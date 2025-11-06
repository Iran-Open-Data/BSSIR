import pandas as pd


def create_year_month_index(from_year: int, to_year: int) -> pd.MultiIndex:
    return pd.MultiIndex.from_product(
        [range(from_year, to_year + 1), range(1, 12 + 1)], names=["Year", "Month"]
    )


def create_year_season_index(from_year: int, to_year: int) -> pd.MultiIndex:
    return pd.MultiIndex.from_product(
        [range(from_year, to_year + 1), range(1, 4 + 1)], names=["Year", "Season"]
    )


def monthly_to_quarterly_by_average(table: pd.DataFrame) -> pd.DataFrame:
    return (
        table
        .reset_index()
        .assign(Season=lambda df: df["Month"].sub(1).floordiv(3).add(1))
        .drop(columns="Month")
        .groupby(["Year", "Season"]).mean()
    )


def sci_cpi_1395_urban_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[2:, [2]]
    table.index = create_year_month_index(1361, 1401)
    table.columns = ["CPI"]
    return table


def sci_cpi_1395_urban_annual(table: pd.DataFrame) -> pd.DataFrame:
    table.columns = ["Year", "CPI"]
    table = table.loc[2:]
    table = table.set_index("Year")
    return table


def sci_cpi_1395_rural_maingroups_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[3], 53:].T
    table.index = create_year_month_index(1374, 1401)
    table.columns = ["CPI"]
    return table


def sci_cpi_1395_rural_maingroups_annual(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[4], 1:].T
    table.columns = ["CPI"]
    table.index = pd.Index(range(1361, 1401), name="Year")
    return table


def sci_cpi_1395_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[3], 1:].transpose()
    table.columns = ["CPI"]
    table.index = create_year_month_index(1390, 1390 + len(table.index) // 12 - 1)
    return table


def sci_cpi_1395_annual(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[4], 1:].transpose().astype("float64")
    table.columns = ["CPI"]
    table.index = pd.Index(range(1390,  1390 + len(table.index)), name="Year")
    return table


def sci_cpi_1395_monthly_urban_rural(table_list: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(
        table_list, keys=["Urban", "Rural"], names=["Urban_Rural", "Year", "Month"]
    )


def sci_cpi_1395_annual_urban_rural(table_list: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(table_list, keys=["Urban", "Rural"], names=["Urban_Rural", "Year"])


def sci_cpi_1400_urban_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[2:, [2]]
    table.index = create_year_month_index(1361, 1361 + len(table.index) // 12 - 1)
    table.columns = ["CPI"]
    return table

def sci_cpi_1400_urban_quarterly(table_list: list[pd.DataFrame]) -> pd.DataFrame:
    return table_list[0].pipe(monthly_to_quarterly_by_average)


def sci_cpi_1400_urban_annual(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[2:]
    table.columns = ["Year", "CPI"]
    table = table.set_index("Year")
    return table


def sci_cpi_1400_rural_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[3], 53:].transpose()
    table.index = create_year_month_index(1374, 1374 + len(table.index) // 12 - 1)
    table.columns = ["CPI"]
    return table


def sci_cpi_1400_rural_quarterly(table: pd.DataFrame) -> pd.DataFrame:
    quarterly_part = table.loc[[3], 2:53].transpose()
    quarterly_part.index = create_year_season_index(1361, 1373)
    quarterly_part.columns = ["CPI"]
    monthly_part = (
        table
        .pipe(sci_cpi_1400_rural_monthly)
        .pipe(monthly_to_quarterly_by_average)
    )
    return pd.concat(
        [quarterly_part, monthly_part],
        names=["Year", "Season"],
    )


def sci_cpi_1400_rural_annual(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[2, 3], 1:].transpose()
    table.columns = ["Year", "CPI"]
    table = table.set_index("Year")
    return table


def sci_cpi_1400_rural_maingroups_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table.loc[1, :] = table.loc[1].ffill()
    table.loc[1, 0] = "Year"
    table.loc[2, 0] = "Month_Seasion"
    table = table.loc[1:]
    table = table.set_index(0)
    table.index.name = None
    table = table.T
    table["Year"] = table["Year"].astype(int)
    table = table.set_index(["Year", "Month_Seasion"])
    table = table.replace(r"[\s\-]", None, regex=True)
    return table


def sci_cpi_1400_rural_maingroups_annual(
    table_list: list[pd.DataFrame],
) -> pd.DataFrame:
    table = table_list[0].groupby("Year").mean()
    return table


def sci_cpi_1400_annual(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[3], 1:].transpose()
    index = pd.Index(range(1390, 1390 + len(table.index)), name="Year")
    table = table.set_axis(index, axis="index").set_axis(["CPI"], axis="columns")
    return table


def sci_cpi_1400_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[3], 1:].transpose()
    index = create_year_month_index(1390, 1390 + len(table.index) // 12 - 1)
    table = table.set_axis(index, axis="index").set_axis(["CPI"], axis="columns")
    return table


def sci_cpi_1400_quarterly(table_list: list[pd.DataFrame]) -> pd.DataFrame:
    return table_list[0].pipe(monthly_to_quarterly_by_average)


def sci_cpi_1400_annual_urban_rural(
    table_list: list[pd.DataFrame],
) -> pd.DataFrame:
    return(
        pd.concat(
            table_list,
            keys=["Urban", "Rural"],
            names=["Urban_Rural", "Year"],
        )
        .reorder_levels(["Year", "Urban_Rural"])
        .sort_index()
    )


def sci_cpi_1400_quarterly_urban_rural(
    table_list: list[pd.DataFrame],
) -> pd.DataFrame:
    return(
        pd.concat(
            table_list,
            keys=["Urban", "Rural"],
            names=["Urban_Rural", "Year", "Season"],
        )
        .reorder_levels(["Year", "Season", "Urban_Rural"])
        .sort_index()
    )


def sci_cpi_1400_monthly_urban_rural(
    table_list: list[pd.DataFrame],
) -> pd.DataFrame:
    return(
        pd.concat(
            table_list,
            keys=["Urban", "Rural"],
            names=["Urban_Rural", "Year", "Month"],
        )
        .reorder_levels(["Year", "Month", "Urban_Rural"])
        .sort_index()
    )


def sci_gini_annual(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[[2], 1:].T
    index = pd.Index(range(1363, 1403), name="Year")
    table = table.set_axis(index, axis="index").set_axis(["Gini"], axis="columns")
    return table


def sci_urban_food_items_price_monthly(table: pd.DataFrame) -> pd.DataFrame:
    table = table.loc[3:].copy()
    table = table.replace("^[-\\s]+$", None, regex=True)
    UNITS = {
        "یک کیلوگرم": "1000",
        "یک و نیم لیتر": "1500",
        "یک لیتر": "1000"
    }
    ITEMS = {
        "برنج ایرانی درجه يك": "Iranian_Rice_Grade_One",
        "برنج خارجي درجه يك": "Imported_Rice_Grade_One",
        "شيريني خشك": "Dry_Pastry",
        "ماكاروني": "Macaroni",
        "رشته آش": "Reshteh_Ash",
        "گوشت گوسفند": "Mutton",
        "گوشت گاو يا گوساله": "Beef_Or_Veal",
        "مرغ ماشيني": "Machine_Raised_Chicken",
        "ماهي قزل آلا": "Trout_Fish",
        "كنسرو ماهي تن": "Canned_Tuna_Fish",
        "شير پاستوريزه": "Pasteurized_Milk",
        "شيرخشك": "Milk_Powder",
        "خامه پاستوريزه": "Pasteurized_Cream",
        "ماست پاستوريزه": "Pasteurized_Yogurt",
        "دوغ پاستوریزه": "Pasteurized_Yogurt_Drink",
        "پنير ايراني پاستوريزه": "Pasteurized_Iranian_Cheese",
        "تخم مرغ ماشيني": "Factory_Farm_Eggs",
        "كره پاستوريزه (کره حیوانی)": "Pasteurized_Butter",
        "روغن مايع": "Liquid_Oil",
        "روغن نباتي جامد": "Solid_Vegetable_Oil",
        "هلو": "Peach",
        "انار": "Pomegranate",
        "موز": "Banana",
        "سيب": "Apple",
        "ليموترش": "Lemon",
        "پرتقال": "Orange",
        "خربزه": "Melon",
        "هندوانه": "Watermelon",
        "كشمش پلويي": "Rice_Raisins",
        "مغز گردو": "Walnut_Kernel",
        "پسته": "Pistachio",
        "مغز بادام درختی": "Almond_Kernel",
        "قارچ": "Mushroom",
        "خيار": "Cucumber",
        "كدو سبز": "Zucchini",
        "بادمجان": "Eggplant",
        "گوجه فرنگي": "Tomato",
        "فلفل دلمه ای": "Bell_Pepper",
        "سيب زميني": "Potato",
        "پياز": "Onion",
        "هويج فرنگي": "Carrot",
        "نخود": "Chickpea",
        "لوبيا چيتي": "Pinto_Beans",
        "لوبيا قرمز": "Red_Beans",
        "عدس": "Lentil",
        "لپه": "Split_Pea",
        "قند": "Cube_Sugar",
        "شكر": "Sugar",
        "رب گوجه فرنگي": "Tomato_Paste",
        "سس گوجه فرنگي": "Tomato_Sauce",
        "سس مايونز": "Mayonnaise_Sauce",
        "چائ خارجي بسته ای": "Packaged_Imported_Tea",
        "نوشابه گازدار": "Carbonated_Drink",
    }
    table[1] = table[1].replace(UNITS).str.replace(" گرم", "").astype(float)
    table[0] = table[0].replace(ITEMS)
    for col in table.columns[2:]:
        table[col] = table[col].astype(float)
    table.loc[:, 2:] = table.loc[:, 2:].mul(1000).div(table.loc[:, 1], axis=0).astype(float)
    table = table.set_index(0)
    table = table.loc[:, 2:].transpose()
    table.index = create_year_month_index(1396, 1404)
    return table


def wb_ppp_conversion_factor(table: pd.DataFrame) -> pd.DataFrame:
    return (
        table
        .loc[lambda df: df["Country Code"].eq("IRN")]
        .transpose()
        .reset_index(names=["Year"])
        .loc[lambda df: df["Year"].astype(str).str.isnumeric()]
        .astype({"Year": int})
        .assign(Year=lambda df: df["Year"].sub(621))
        .dropna()
        .set_index(["Year"])
        .set_axis(["PPP_Conversion_Factor"], axis="columns")
    )
