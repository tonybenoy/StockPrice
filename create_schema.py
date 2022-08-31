import logging

from sqlalchemy import (
    Column,
    Date,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
    UniqueConstraint,
)

from extensions import engine
from model import Base

logging.getLogger().setLevel(logging.INFO)

# If you have modified models:
# - Delete or rename existing "mini.db" file
# - Run this script

if __name__ == "__main__":
    Base.metadata.clear()
    logging.info("Creating the database schema..")
    valid_currency = Table(
        "valid_currency",
        Base.metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
        Column("currency_symbol", String, nullable=False),
    )
    exchange_rate_usd = Table(
        "exchange_rate_usd",
        Base.metadata,
        Column("id", Integer, primary_key=True),
        Column("currency", Integer, ForeignKey("valid_currency.id"), nullable=False),
        Column("rate", Float, nullable=False),
        Column("date", Date, nullable=False),
        UniqueConstraint("date", "currency", name="date_currency_unique"),
    )
    market_data = Table(
        "market_data",
        Base.metadata,
        Column("id", Integer, primary_key=True),
        Column("date", Date, nullable=False),
        Column("symbol", String, nullable=False),
        Column("close_price_usd", Integer, nullable=False),
        Column("close_price_cent", Integer, nullable=False),
        UniqueConstraint("date", "symbol", name="date_symbol_unique"),
    )
    Base.metadata.create_all(engine)
