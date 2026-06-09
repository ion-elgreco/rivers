"""Pipeline with a slow asset that writes a marker file on completion.

Env vars:
    MARKER_PATH — path to the file written when the asset finishes
"""

import os
import time

import rivers as rs


class _MarkerIO(rs.BaseIOHandler):
    def handle_output(self, context, obj):
        pass

    def load_input(self, context):
        return None


_io = _MarkerIO()


@rs.Asset(io_handler=_io)
def slow_asset():
    time.sleep(5)
    with open(os.environ["MARKER_PATH"], "w") as f:
        f.write("done")
    return 1


repo = rs.CodeRepository(assets=[slow_asset], default_executor=rs.Executor.in_process())
