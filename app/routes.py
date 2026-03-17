from datetime import datetime
from functools import wraps

from flask import request, jsonify

from app.services import ingest_file, get_positions, check_concentration, reconcile, check_health, IngestionError


def require_date(f):
    """Decorator that parses the 'date' query param and passes it to the route."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        date_str = request.args.get("date")
        if not date_str:
            return jsonify({"error": "'date' query param required"}), 400
        try:
            kwargs["date"] = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": f"Invalid date format: '{date_str}', expected YYYY-MM-DD"}), 400
        return f(*args, **kwargs)
    return wrapper


def register_routes(app):

    @app.route("/ping", methods=["GET"])
    def ping():
        try:
            return jsonify(check_health()), 200
        except Exception as e:
            return jsonify({"status": "unhealthy", "error": str(e)}), 503

    @app.route("/ingest", methods=["POST"])
    def ingest():
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400

        mode = request.args.get("mode", "strict").lower()
        strict = mode != "permissive"
        results = []

        for uploaded in request.files.getlist("file"):
            content = uploaded.read().decode("utf-8")
            try:
                report = ingest_file(uploaded.filename, content, strict=strict)
                results.append(report)
            except IngestionError as e:
                return jsonify({"error": str(e)}), 422

        return jsonify({"files": results}), 200

    @app.route("/positions", methods=["GET"])
    @require_date
    def positions(date):
        account = request.args.get("account")
        if not account:
            return jsonify({"error": "'account' query param required"}), 400

        return jsonify(get_positions(account, date)), 200

    @app.route("/compliance/concentration", methods=["GET"])
    @require_date
    def compliance_concentration(date):
        return jsonify(check_concentration(date)), 200

    @app.route("/reconciliation", methods=["GET"])
    @require_date
    def reconciliation(date):
        return jsonify(reconcile(date)), 200
