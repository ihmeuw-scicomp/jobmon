"""Initialize Web services."""
from jobmon.server.web.app_factory import AppFactory  # noqa F401
from jobmon.server.web.models import load_model
from flask_cors import CORS

load_model()
_app_factory = AppFactory()
app = _app_factory.get_app(blueprints=["cli"], url_prefix="/api")
CORS(app)

if __name__ == "__main__":
    with app.app_context():
        app.run(host="0.0.0.0", port=8070)
