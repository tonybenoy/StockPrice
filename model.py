from typing import Any

from sqlalchemy import Column, Date, Float, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base: Any = declarative_base()


class Example(Base):
    __tablename__ = "example"
    id = Column(Integer, primary_key=True)
    column_a = Column(String)
    column_b = Column(String)


class Currency(Base):
    __tablename__ = "valid_currency"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    currency_symbol = Column(String, unique=True, nullable=False)


class ExchangeRate(Base):
    __tablename__ = "exchange_rate_usd"
    id = Column(Integer, primary_key=True)
    currency = Column(Integer, ForeignKey("valid_currency.id"))
    rate = Column(Float)
    date = Column(Date)
    symbol = relationship("Currency", backref="exchange_rate_usd")


class MarketData(Base):
    __tablename__ = "market_data"
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    symbol = Column(String)
    close_price_usd = Column(Integer)
    close_price_cent = Column(Integer)
