import logging
from datetime import date
from typing import Any, Dict, List

import arrow
import pandas as pd
import requests
import typer
from rich import print as rprint
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from constants import (
    APILAYER_API_KEY,
    EXCHANGE_RATE_BASE_URL,
    MARKETSTACK_API_KEY,
    MARKETSTACK_BASE_URL,
)
from extensions import session
from model import Currency, ExchangeRate, MarketData
from utils import get_business_days, get_currency_list, handle_api_error, log_and_exit

logging.getLogger().setLevel(logging.INFO)


app = typer.Typer(pretty_exceptions_show_locals=False)


@app.command()
def main(
    symbol=typer.Option(...),
    currency: str = typer.Option(...),
    start_date=typer.Option(...),
    last_date=typer.Option(...),
):
    try:
        start_date = arrow.get(start_date).date()
    except Exception as e:
        log_and_exit(f"{start_date} is not a valid date,{e}", 1)
    try:
        last_date = arrow.get(last_date).date()
    except Exception as e:
        log_and_exit(f"{last_date} is not a valid date,{e}", 1)
    try:
        symbol = symbol.upper()
    except Exception as e:
        log_and_exit(f"{symbol} is not a valid symbol,{e}", 1)

    try:
        currency = currency.upper()
    except Exception as e:
        log_and_exit(f"{currency} is not a valid string currency,{e}", 1)

    if not validate_data(currency=currency, start_date=start_date, last_date=last_date):
        log_and_exit("Fix the above errors and try again", 1)

    if currency != "USD":
        if not get_exchange_rates(
            start_date=start_date, end_date=last_date, currency=currency
        ):
            log_and_exit("Exchange rates not available for the given currency", 1)

    if not get_market_data(symbol=symbol, start_date=start_date, last_date=last_date):
        log_and_exit("Market data not available for the given symbol", 1)

    output(currency=currency, start_date=start_date, last_date=last_date, symbol=symbol)


def output(currency: str, start_date: date, last_date: date, symbol: str):
    # Output the data in a table
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        progress.add_task(description="Processing the information", total=None)
        market_df = pd.read_sql(
            session.query(MarketData)
            .filter(
                MarketData.symbol == symbol,
                MarketData.date >= start_date,
                MarketData.date <= last_date,
            )
            .statement,
            session.bind,
        )
        exchange_df = pd.read_sql(
            session.query(ExchangeRate, Currency.currency_symbol)
            .join(Currency)
            .filter(
                ExchangeRate.symbol.has(Currency.currency_symbol == currency),
                ExchangeRate.date >= start_date,
                ExchangeRate.date <= last_date,
            )
            .statement,
            session.bind,
        )

        df = market_df.merge(exchange_df, on=["date"], how="left")
        df["close_price"] = (
            df["close_price_usd"] + (df["close_price_cent"] / 100)
        ) * df["rate"]
        df = df.drop(
            [
                "id_x",
                "close_price_usd",
                "close_price_cent",
                "close_price_usd",
                "currency",
                "id_y",
                "rate",
            ],
            axis=1,
        )
        table = Table(
            title=f"Market Data for {symbol} in\
                 {currency} from {start_date} to {last_date}"
        )
        for heading in df.head():
            table.add_column(
                str(heading).capitalize(), justify="right", style="cyan", no_wrap=True
            )
        for row in df.iterrows():
            items = [
                str(item) if type(item) != date else item.strftime("%Y-%m-%d")
                for item in row[1]
            ]
            table.add_row(*items)

    console = Console()
    console.print(table)


def validate_dates(start_date: date, last_date: date) -> bool:
    # Check if start date is less than last date
    # Check if start date is less than current date
    # Check if last date is less than current date
    # Check if number of days between start date and
    # last date is less than 365 days
    if last_date < start_date:
        log_and_exit(f"{last_date} is less than {start_date}", 1)
    today = arrow.utcnow().date()
    if start_date > today:
        log_and_exit(f"{start_date} is greater than {today}", 1)
    if last_date > today:
        log_and_exit(f"{last_date} is greater than {today}", 1)
    if (last_date - start_date).days > 365:
        rprint(f"{(last_date - start_date).days} is greater than 365", 1)
    return True


def update_symbols_from_endpoint() -> Dict[str, str]:
    # Call the api and get the updated
    # list of currencies to check if new currency
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        progress.add_task(description="Checking for new currencies", total=None)
        response = requests.request(
            "GET",
            f"{EXCHANGE_RATE_BASE_URL}symbols",
            data="",
            headers={"apikey": APILAYER_API_KEY},
        )
        if response.status_code == 200:
            return response.json()["symbols"]
        else:
            handle_api_error(response=response)


def update_currency_list() -> List[str]:
    # Call the api and get the updated list of currencies to check if new currency
    # is availableIf new currency is available, add it to the list
    # of valid currencies
    currencies = update_symbols_from_endpoint()
    available_currencies = get_currency_list()
    for symbol, name in currencies.items():
        if symbol not in available_currencies:
            new_currency = Currency(currency_symbol=symbol, name=name)
            session.add(new_currency)
    session.commit()
    return get_currency_list()


def validate_currency(currency: str) -> bool:
    # Check if currency is in the list of valid currencies
    # If not, call the currency list api and update the list
    # of valid currencies
    if currency not in get_currency_list():
        currencies = update_currency_list()
        if currency not in currencies:
            logging.info(f"{currency} is not a valid currency")
            return False
    return True


def validate_data(currency: str, start_date: date, last_date: date) -> bool:
    # Validate the data to check for issues
    if validate_dates(start_date=start_date, last_date=last_date):
        logging.debug("Valid dates have been passed and accepted")
        rprint("Valid dates have been passed and accepted")
    if validate_currency(currency=currency):
        logging.info("Valid currency has been passed and accepted")
        rprint("Valid currency has been passed and accepted")
    return True


def update_exchange_rates(start_date: date, end_date: date) -> Dict[str, Any]:
    # Get the exchange rates for the given currency and date range
    querystring = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "base": "USD",
    }
    logging.debug("Updating exchange rates")

    response = requests.request(
        "GET",
        f"{EXCHANGE_RATE_BASE_URL}timeseries",
        data="",
        headers={"apikey": APILAYER_API_KEY},
        params=querystring,
    )
    if response.status_code == 200:
        return response.json()["rates"]
    else:
        handle_api_error(response=response)


def get_exchange_rates(start_date: date, end_date: date, currency: str):
    # Get the exchange rates for the given currency and date range
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        progress.add_task(
            description="Checking if exchange rates available localy", total=None
        )
        qry = (
            session.query(ExchangeRate)
            .filter(
                ExchangeRate.date.between(start_date, end_date),
                ExchangeRate.symbol.has(Currency.currency_symbol == currency),
            )
            .all()
        )
        if len(qry) == ((end_date - start_date).days + 1):
            logging.debug(f"{currency} exchange rates are already available")
            rprint(f"{currency} exchange rates are already available")
            return True
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        progress.add_task(description="Getting updated exchange rates", total=None)
        exchange_rates = update_exchange_rates(start_date=start_date, end_date=end_date)
        existing_dates = [exchange_data.date for exchange_data in qry]
        available_currencies = {
            currency: id
            for currency, id in session.query(
                Currency.currency_symbol, Currency.id
            ).all()
        }
        logging.debug("Getting exchange rates")
        for _date, rates in exchange_rates.items():
            date = arrow.get(_date).date()
            for exchange_currency, rate in rates.items():
                if (
                    exchange_currency in available_currencies
                    and date not in existing_dates
                ):
                    currency_id = available_currencies[exchange_currency]
                    new_exchange_rate = ExchangeRate(
                        date=date,
                        currency=currency_id,
                        rate=rate,
                    )
                    session.add(new_exchange_rate)
    session.commit()
    return True


def get_market_data(symbol: str, start_date: date, last_date: date) -> bool:
    # Get the market data for the given symbol and date range
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        progress.add_task(
            description="Checking if market data available localy", total=None
        )
        qry = (
            session.query(MarketData).filter(
                MarketData.date.between(start_date, last_date),
                MarketData.symbol == symbol,
            )
        ).all()
        if len(qry) == get_business_days(start_date=start_date, end_date=last_date):
            logging.debug(f"{symbol} market data is already available")
            rprint(f"{symbol} market data is already available")
            return True
    available_dates = [market_data.date for market_data in qry]
    querystring = {
        "access_key": MARKETSTACK_API_KEY,
        "symbols": symbol,
        "date_from": start_date.strftime("%Y-%m-%d"),
        "date_to": last_date.strftime("%Y-%m-%d"),
        "limit": "1000",
    }
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        progress.add_task(description="Getting updated closing price", total=None)
        while True:
            response = requests.request(
                "GET", f"{MARKETSTACK_BASE_URL}eod", data="", params=querystring
            )
            resp_json = {}
            if response.status_code == 200:
                resp_json = response.json()
                for data in resp_json["data"]:
                    date = arrow.get(data["date"]).date()
                    if date not in available_dates:
                        new_market_data = MarketData(
                            date=date,
                            symbol=data["symbol"],
                            close_price_usd=int(data["close"]),
                            close_price_cent=int((data["close"] % 1) * 100),
                        )
                        session.add(new_market_data)
                offset = resp_json["pagination"]["offset"]
                limit = resp_json["pagination"]["limit"]
                total = resp_json["pagination"]["total"]
                count = resp_json["pagination"]["count"]
                if offset + count < total:
                    querystring.update({"offset": offset + limit})
                else:
                    break
            if response.status_code == 422:
                log_and_exit(f"{symbol} symbol is not valid", 1)
            else:
                handle_api_error(response=response)
    session.commit()
    return True


if __name__ == "__main__":
    app()
