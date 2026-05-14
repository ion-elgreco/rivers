"""Minimal pipeline with a single no-op asset."""

import rivers as rs


class _NullIO(rs.BaseIOHandler):
    def handle_output(self, context, obj):
        pass

    def load_input(self, context):
        return None


@rs.Asset(io_handler=_NullIO())
def noop():
    return 1


repo = rs.CodeRepository(assets=[noop])
