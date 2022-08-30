from typing import Any

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base: Any = declarative_base()


class Example(Base):
    __tablename__ = "example"
    id = Column(Integer, primary_key=True)
    column_a = Column(String)
    column_b = Column(String)
