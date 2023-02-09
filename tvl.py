import pandas as pd

BASE_TEXT = """
CREATE OR REPLACE VIEW dune_user_generated.second_feb_tvl
(chain,
protocol,
tvl) AS VALUES

"""

def get_gaming_content():
    all_data = pd.read_csv("tvl.csv")

    all_data['chain'] = '(\'' + all_data['chain'].astype(str) + '\'' + "::text, "
    all_data['protocol'] = all_data['protocol'].replace(to_replace= r'\'', value= '', regex=True)
    all_data['protocol'] = '\'' + all_data['protocol'].astype(str) + '\'' + "::text, "
    all_data['tvl'] = all_data['tvl'].astype(str)  + "::bigint)"
    all_data['combined'] = all_data['chain'] + all_data['protocol'] + all_data['tvl']

    df = all_data['combined'].to_csv(header=None, index=False).strip('\n').split('\n')
    return BASE_TEXT + '\r\n,'.join(df).replace('"', '')

print(get_gaming_content())