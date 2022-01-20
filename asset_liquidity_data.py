import requests
import json
import pandas as pd
from datetime import datetime

astroport_factory_address = "terra1fnywlw4edny3vw44x04xd67uzkdqluymgreu7g"
terraswap_factory_address = "terra1ulgw0td86nvs4wtpsc80thv6xelk76ut7a7apj"

assets_file = open("assets.json", "r")
assets = json.load(assets_file)


def get_pair_liquidity(factory_address, asset_address):
    pair_liquidity = 0
    try:
        pair = requests.get(
            f"https://lcd.terra.dev/wasm/contracts/{factory_address}/store?query_msg={{%22pair%22:{{%22asset_infos%22:[{{%22token%22:{{%22contract_addr%22:%22{asset_address}%22}}}},{{%22native_token%22:{{%22denom%22:%22uusd%22}}}}]}}}}"
        ).json()
        pair_address = pair["result"]["contract_addr"]
        pool = requests.get(
            f"https://lcd.terra.dev/wasm/contracts/{pair_address}/store?query_msg=%7B%22pool%22:%7B%7D%7D"
        ).json()
        pool_assets = pool["result"]["assets"]
        native_amount = 0
        token_amount = 0
        for pool_asset in pool_assets:
            if "token" in pool_asset["info"]:
                token_amount = int(pool_asset["amount"])
            elif "native_token" in pool_asset["info"]:
                assert pool_asset["info"]["native_token"]["denom"] == "uusd"
                native_amount = int(pool_asset["amount"])
        pair_liquidity = (native_amount * 2) / 1e6
    except:
        pass
    return pair_liquidity


def main():
    asset_symbols = assets.keys()
    asset_addresses = assets.values()
    asset_terraswap_liquidity = []
    asset_astroport_liquidity = []
    for (asset_symbol, asset_address) in assets.items():
        pair_liquidity = get_pair_liquidity(terraswap_factory_address, asset_address)
        asset_terraswap_liquidity.append(pair_liquidity)
        pair_liquidity = get_pair_liquidity(astroport_factory_address, asset_address)
        asset_astroport_liquidity.append(pair_liquidity)

    asset_data_sources = []
    df = pd.DataFrame(
        {
            "asset_symbols": asset_symbols,
            "asset_address": asset_addresses,
            "asset_terraswap_liquidity": asset_terraswap_liquidity,
            "asset_astroport_liquidity": asset_astroport_liquidity,
        }
    )
    current_date = datetime.now()
    formatted_date = current_date.strftime("%d_%m_%y")
    df.to_csv(f"asset_liquidity_info/asset_liquidity_info_{formatted_date}.csv", index=False)


if __name__ == "__main__":
    main()
