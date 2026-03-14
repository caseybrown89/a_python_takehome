from sqlalchemy.dialects.postgresql import ARRAY

from app import db

# Note: Tables are created via db.create_all() in the app factory, which only
# handles initial creation -- it cannot alter existing tables. In a production
# system, Flask-Migrate (Alembic) would be used to manage schema evolution
# through versioned migration scripts.


class Trade(db.Model):
    __tablename__ = "trade"
    __table_args__ = (
        db.UniqueConstraint(
            'trade_date', 'account_id', 'ticker', 'quantity', 'price', 'trade_type',
            name='uq_trade_natural_key'
        ),
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


class Position(db.Model):
    __tablename__ = "position"

    id = db.Column(db.Integer, primary_key=True)
    report_date = db.Column(db.Date, nullable=False)
    account_id = db.Column(db.String(20), nullable=False)
    ticker = db.Column(db.String(10), nullable=False)
    shares = db.Column(db.Numeric(20, 6), nullable=False)
    market_value = db.Column(db.Numeric(20, 6), nullable=False)
    custodian_ref = db.Column(db.String(50), nullable=False)
