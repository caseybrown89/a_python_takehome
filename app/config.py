import os

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/clearinghouse"
)
SQLALCHEMY_TRACK_MODIFICATIONS = False
DEBUG = os.environ.get("FLASK_DEBUG", "false").lower() in ("true", "1")
