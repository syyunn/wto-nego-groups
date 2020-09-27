import psycopg2
import data.raw.unmtrd.auth as auth
import pandas as pd
from pathlib import Path

membershipPath = '../../label/membership.csv'
mmbrshp = pd.read_csv(membershipPath)
mmbr_codes = mmbrshp['code'].to_list()
IM_EX = ['Import', 'Export']

connection = psycopg2.connect(
    user=auth.user,
    password=auth.password,
    host=auth.host,
    port=auth.port,
    database=auth.database,
)  # please make sure to run ssh -L <local_port>:localhost:<remote_port> <user_at_remote>@<remote_address>

conn_status = connection.closed  # 0

years = [i for i in range(1988, 2020)]

for year in years:
    for code in mmbr_codes:
        try:
            Path(f"./Export/{code}").mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            pass

        try:
            Path(f"./Import/{code}").mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            pass

        for imex in IM_EX:
            try:
                crsr = connection.cursor()
                crsr.execute(
                    f"""
                        select * from raw___uncmtrd.annual
                        where aggregate_level = 2 
                        and year = {year} 
                        and reporter_code = {code} 
                        and trade_flow = '{imex}' 
                        and partner_code = 0;
                """
                )
                df = pd.DataFrame(crsr.fetchall())
                # print(df)
                df.to_csv(
                    f"./{imex}/{code}/y{year}_agg2_imex{imex}_rpc{code}.csv",
                    sep=",",
                    index=False,
                )
                print("done", year, imex, code)

            except psycopg2.OperationalError:
                print("error occurred", year)
                pass

if __name__ == "__main__":
    pass
