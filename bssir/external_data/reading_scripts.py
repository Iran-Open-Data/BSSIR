import json
from pathlib import Path
from io import BytesIO
from zipfile import ZipFile

import pandas as pd


def wb_ppp_conversion_factor(path: Path) -> pd.DataFrame:
    csv_ziped = ZipFile(path)
    data_files = [f for f in csv_ziped.filelist if not "Metadata" in f.filename]
    assert len(data_files) == 1
    return pd.read_csv(BytesIO(csv_ziped.read(data_files[0])), skiprows=range(4))


def imf_ppp_conversion_factor(path: Path) -> pd.DataFrame:
    with path.open(encoding="utf-8") as file:
        json_content = json.load(file)
    return(
        pd.Series(json_content["values"]["PPPEX"]["IRN"])
        .to_frame("PPP_Conversion_Factor")
        .reset_index(names=["Year"])
        .assign(Year=lambda df: df["Year"].astype(int).sub(621))
        .set_index("Year")
        .astype(float)
    )
