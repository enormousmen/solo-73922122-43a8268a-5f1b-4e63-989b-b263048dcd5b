"""Production entrypoint.  Runs schema bootstrap exactly once per worker."""

import os

from alertsvc.app import create_app

app = create_app()
app.config["INIT_DB"]()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
