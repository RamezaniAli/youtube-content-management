from flask import request, abort
from airflow.plugins_manager import AirflowPlugin


def block_robots():
    if request.path == '/robots.txt':
        abort(403)


class BlockRobotsPlugin(AirflowPlugin):
    name = "block_robots"

    def on_load(self, app):
        app.before_request(block_robots)
