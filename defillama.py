import datetime as dt
from datetime import datetime
import brotli
import pandas as pd
import numpy as np
import requests


def daily_protocol_chain_tvl_ingest():
    url = "https://api.llama.fi/lite/protocols2"
    response = requests.get(url)
    dict = response.json()
    names_list = []
    chains_list = []
    tvl_list = []
    try:
        for prot in dict["protocols"]:
            for chain in prot["chainTvls"]:
                if "doublecounted" in chain:
                    pass
                else:
                    names_list.append(prot["name"])
                    chains_list.append(chain)
                    tvl_list.append(prot["chainTvls"][chain]["tvlPrevDay"])
    except Exception as e:
        logging.warning(e)
        pass

    df = pd.DataFrame(
        {"protocol": names_list, "chain": chains_list, "tvl_usd": tvl_list}
    )

    df = df.loc[~df["tvl_usd"].isnull()].copy()
    df["tvl_usd"] = df["tvl_usd"].astype(int)
    df["date_time"] = dt.date.today()
    df = df[["date_time", "protocol", "chain", "tvl_usd"]].copy()
    df.to_csv("tvl.csv")


if __name__ == "__main__":
    daily_protocol_chain_tvl_ingest()
