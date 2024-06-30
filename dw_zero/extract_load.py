from sqlalchemy import create_engine, URL
from dotenv import load_dotenv
import os
from yfinance import Ticker
import pandas as pd
import concurrent


def buscar_dados_commodities(
    simbolo: str,
    period: str = "5d",
    interval: str = "1d",
) -> pd.DataFrame:
    """Fetch commodity data.

    Args:
        simbolo (str): Commodity symbol.
        period (str): Time period. Defaults to "5d".
        interval (str): Time interval. Defaults to "1d".

    Returns:
        pd.DataFrame: Commidty data.
    """
    ticker = Ticker(simbolo)

    data: pd.DataFrame = ticker.history(period=period, interval=interval)

    data["symbol"] = simbolo

    return data


def buscar_todos_dados_commodities() -> pd.DataFrame:
    """Fetch commodity data for all symbols in parallel."""
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(buscar_dados_commodities, simbolo)
            for simbolo in commodities
        ]
        data_frames = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]
    return pd.concat(data_frames)


def salvar_no_postgres(df: pd.DataFrame, schema: str = "public") -> str:
    """
    Save a pandas DataFrame to a PostgreSQL database efficiently.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        schema (str, optional): The schema in the database to save the DataFrame to. Defaults to "public".

    Returns:
        str: A message indicating the success of the data save operation.
    """
    db_params = {
        "user": os.getenv("DB_USER_PROD"),
        "password": os.getenv("DB_PASSWORD_PROD"),
        "host": os.getenv("DB_HOST_PROD"),
        "port": os.getenv("DB_PORT_PROD"),
        "database": os.getenv("DB_NAME_PROD"),
    }
    engine = create_engine(
        URL.create("postgresql", **db_params),
        fast_executemany=True,
        isolation_level="AUTOCOMMIT",
    )

    with engine.begin() as conn:
        df.to_sql(
            "commodities",
            conn,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
        )

    return f'Dados salvos com sucesso em {os.getenv("DB_NAME_PROD")}'


if __name__ == "__main__":
    load_dotenv()

    commodities = ["CL=F", "GC=F", "SI=F"]

    df = buscar_todos_dados_commodities()

    print(salvar_no_postgres(df.reset_index()))
