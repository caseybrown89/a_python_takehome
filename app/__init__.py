from decimal import Decimal

from flask import Flask
from flask.json.provider import DefaultJSONProvider
from flask_sqlalchemy import SQLAlchemy


class _JSONProvider(DefaultJSONProvider):
    """Extend Flask's default JSON provider to serialize Decimal values."""

    @staticmethod
    def default(o):
        if isinstance(o, Decimal):
            return float(o)
        return DefaultJSONProvider.default(o)

db = SQLAlchemy()


def create_app(config_override=None):
    app = Flask(__name__)
    app.json_provider_class = _JSONProvider
    app.json = _JSONProvider(app)
    app.config.from_object("app.config")

    if config_override:
        app.config.update(config_override)

    db.init_app(app)

    from app.routes import register_routes
    register_routes(app)

    with app.app_context():
        from app import models  # noqa: F401
        db.create_all()

    return app
