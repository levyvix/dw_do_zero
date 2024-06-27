from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from yfinance import Ticker
import pandas as pd

commodities = ["CL=F", "GC=F", "SI=F"]


def buscar_dados_commodities(simbolo, period="5d", invervalo: str = "1d"):
    ticker = Ticker(simbolo)

    dados: pd.DataFrame = ticker.history(period=period, interval=invervalo)

    dados["symbol"] = simbolo

    return dados


def buscar_todos_dados_commodities():
    return pd.concat([buscar_dados_commodities(simbolo) for simbolo in commodities])


def salvar_no_postgres(df, schema="public"):
    engine = create_engine(
        f'postgresql+psycopg2://{os.getenv("DB_USER_PROD")}:{os.getenv("DB_PASSWORD_PROD")}@{os.getenv("DB_HOST_PROD")}:{os.getenv("DB_PORT_PROD")}/{os.getenv("DB_NAME_PROD")}'
    )

    df.to_sql("commodities", engine, schema=schema, if_exists="append", index=False)

    return f'Dados salvos com sucesso em {os.getenv("DB_NAME_PROD")}'


if __name__ == "__main__":
    load_dotenv()

    df = buscar_todos_dados_commodities()

    print(salvar_no_postgres(df.reset_index()))
