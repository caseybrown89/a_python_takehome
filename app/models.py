from abc import abstractmethod

from sqlalchemy import any_, case, func, literal_column
from sqlalchemy.dialects.postgresql import ARRAY, insert as pg_insert

from app import db


# Note: Ingestible cannot inherit from ABC directly because db.Model uses
# SQLAlchemy's DeclarativeMeta metaclass, which conflicts with ABCMeta.
# We use @abstractmethod without ABC as a convention-based interface instead.
class Ingestible:
    @abstractmethod
    def process(self) -> tuple[int, int]:
        """Persist this record. Returns (inserted_count, updated_count)."""
        ...

# Note: Tables are created via db.create_all() in the app factory, which only
# handles initial creation -- it cannot alter existing tables. In a production
# system, Flask-Migrate (Alembic) would be used to manage schema evolution
# through versioned migration scripts.


class Trade(db.Model, Ingestible):
    __tablename__ = "trade"
    __table_args__ = (
        db.UniqueConstraint(
            'trade_date', 'account_id', 'ticker', 'quantity', 'price', 'trade_type',
            name='uq_trade_natural_key'
        ),
        db.Index('ix_trade_account_date', 'account_id', 'trade_date'),
    )

    id = db.Column(db.Integer, primary_key=True)
    trade_date = db.Column(db.Date, nullable=False)
    account_id = db.Column(db.String(20), nullable=False)
    ticker = db.Column(db.String(10), nullable=False)
    quantity = db.Column(db.Numeric(20, 6), nullable=False)
    price = db.Column(db.Numeric(20, 6), nullable=False)
    market_value = db.Column(db.Numeric(20, 6), nullable=False)
    trade_type = db.Column(db.String(10), nullable=False)
    settlement_date = db.Column(db.Date, nullable=True)
    custodian = db.Column(db.String(50), nullable=True)
    source_file = db.Column(ARRAY(db.String(50)), nullable=False)

    def process(self):
        stmt = pg_insert(Trade).values(
            trade_date=self.trade_date,
            account_id=self.account_id,
            ticker=self.ticker,
            quantity=self.quantity,
            price=self.price,
            market_value=self.market_value,
            trade_type=self.trade_type,
            settlement_date=self.settlement_date,
            custodian=self.custodian,
            source_file=self.source_file,
        )
        new_filename = stmt.excluded.source_file[1]
        stmt = stmt.on_conflict_do_update(
            constraint='uq_trade_natural_key',
            set_={
                'settlement_date': func.coalesce(
                    stmt.excluded.settlement_date, Trade.settlement_date
                ),
                'custodian': func.coalesce(
                    stmt.excluded.custodian, Trade.custodian
                ),
                'source_file': case(
                    (new_filename == any_(Trade.source_file), Trade.source_file),
                    else_=func.array_append(Trade.source_file, new_filename),
                ),
            },
        ).returning(literal_column('(xmax = 0)').label('was_inserted'))
        was_inserted = db.session.execute(stmt).scalar()
        return (1, 0) if was_inserted else (0, 1)


class Position(db.Model, Ingestible):
    __tablename__ = "position"
    __table_args__ = (
        db.UniqueConstraint(
            'report_date', 'account_id', 'ticker',
            name='uq_position_natural_key'
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    report_date = db.Column(db.Date, nullable=False)
    account_id = db.Column(db.String(20), nullable=False)
    ticker = db.Column(db.String(10), nullable=False)
    shares = db.Column(db.Numeric(20, 6), nullable=False)
    market_value = db.Column(db.Numeric(20, 6), nullable=False)
    custodian_ref = db.Column(db.String(50), nullable=False)

    def process(self):
        stmt = pg_insert(Position).values(
            report_date=self.report_date,
            account_id=self.account_id,
            ticker=self.ticker,
            shares=self.shares,
            market_value=self.market_value,
            custodian_ref=self.custodian_ref,
        )
        stmt = stmt.on_conflict_do_update(
            constraint='uq_position_natural_key',
            set_={
                'shares': stmt.excluded.shares,
                'market_value': stmt.excluded.market_value,
                'custodian_ref': stmt.excluded.custodian_ref,
            },
        ).returning(literal_column('(xmax = 0)').label('was_inserted'))
        was_inserted = db.session.execute(stmt).scalar()
        return (1, 0) if was_inserted else (0, 1)
