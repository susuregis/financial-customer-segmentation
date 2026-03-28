from datetime import datetime

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from src.config.config import Config


Base = declarative_base()


class DateDim(Base):
    __tablename__ = "date_dim"

    date_id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, unique=True, nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)
    day_of_week = Column(String(20), nullable=False)
    is_weekend = Column(Integer, nullable=False)

    transactions = relationship("TransactionFact", back_populates="date")


class CurrencyDim(Base):
    __tablename__ = "currency_dim"

    currency_id = Column(Integer, primary_key=True, autoincrement=True)
    currency_code = Column(String(3), unique=True, nullable=False)
    currency_name = Column(String(100), nullable=False)
    country = Column(String(100), nullable=False)

    transactions = relationship("TransactionFact", back_populates="currency")
    exchange_rates = relationship("ExchangeRateFact", back_populates="currency")


class CustomerDim(Base):
    __tablename__ = "customer_dim"

    customer_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_code = Column(String(50), unique=True, nullable=False)
    customer_name = Column(String(150), nullable=False)
    country = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    ml_cluster = Column(Integer, nullable=True)

    transactions = relationship("TransactionFact", back_populates="customer")


class CategoryDim(Base):
    __tablename__ = "category_dim"

    category_id = Column(Integer, primary_key=True, autoincrement=True)
    category_code = Column(String(50), unique=True, nullable=False)
    category_name = Column(String(150), nullable=False)
    description = Column(String(500), nullable=True)

    accounts = relationship("AccountDim", back_populates="category")


class AccountDim(Base):
    __tablename__ = "account_dim"

    account_id = Column(Integer, primary_key=True, autoincrement=True)
    account_code = Column(String(80), unique=True, nullable=False)
    account_type = Column(String(50), nullable=False)
    category_id = Column(Integer, ForeignKey("category_dim.category_id"), nullable=False)

    category = relationship("CategoryDim", back_populates="accounts")
    transactions = relationship("TransactionFact", back_populates="account")


class TransactionFact(Base):
    __tablename__ = "transaction_fact"

    transaction_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, ForeignKey("customer_dim.customer_id"), nullable=True)
    account_id = Column(Integer, ForeignKey("account_dim.account_id"), nullable=False)
    currency_id = Column(Integer, ForeignKey("currency_dim.currency_id"), nullable=False)
    date_id = Column(Integer, ForeignKey("date_dim.date_id"), nullable=False)
    amount_original = Column(Numeric(18, 2), nullable=False)
    amount_brl = Column(Numeric(18, 2), nullable=False)
    exchange_rate = Column(Numeric(10, 4), nullable=False)
    transaction_date = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    customer = relationship("CustomerDim", back_populates="transactions")
    account = relationship("AccountDim", back_populates="transactions")
    currency = relationship("CurrencyDim", back_populates="transactions")
    date = relationship("DateDim", back_populates="transactions")


class ExchangeRateFact(Base):
    __tablename__ = "exchange_rate_fact"
    __table_args__ = (
        UniqueConstraint("currency_id", "date", name="uq_exchange_rate_currency_date"),
    )

    rate_id = Column(Integer, primary_key=True, autoincrement=True)
    currency_id = Column(Integer, ForeignKey("currency_dim.currency_id"), nullable=False)
    date = Column(Date, nullable=False)
    buy_rate = Column(Numeric(10, 4), nullable=False)
    sell_rate = Column(Numeric(10, 4), nullable=False)
    average_rate = Column(Numeric(10, 4), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    currency = relationship("CurrencyDim", back_populates="exchange_rates")


class DatabaseManager:
    def __init__(self, database_url: str = None):
        self.database_url = database_url or Config().DATABASE_URL
        self.engine = create_engine(self.database_url, echo=False, future=True)
        self.Session = sessionmaker(bind=self.engine)

    def create_all_tables(self):
        Base.metadata.create_all(self.engine)

    def drop_all_tables(self):
        Base.metadata.drop_all(self.engine)

    def get_session(self):
        return self.Session()

    def close(self):
        self.engine.dispose()
