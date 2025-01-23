from flask import request, abort
from airflow.plugins_manager import AirflowPlugin


def block_robots_middleware(app):
    @app.before_request
    def block_robots():
        if request.path == '/robots.txt':
            abort(403)


class BlockRobotsPlugin(AirflowPlugin):
    name = "block_robots"

    def on_load(self, app):
        block_robots_middleware(app)
