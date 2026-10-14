"""Built-in IO handlers used to persist and load asset values.

Always available: :class:`BaseIOHandler`, :class:`InMemoryIOHandler`,
:class:`PickleIOHandler`. The Delta Lake handler is exported only when
``deltalake`` is installed (``pip install rivers[delta]``), and the Iceberg
handler only when ``pyiceberg`` is installed (``pip install rivers[iceberg]``).
"""

from rivers.io_handlers.base import BaseIOHandler
from rivers.io_handlers.memory import InMemoryIOHandler
from rivers.io_handlers.pickle import PickleIOHandler

__all__ = ["BaseIOHandler", "InMemoryIOHandler", "PickleIOHandler"]

try:
    from rivers.io_handlers.delta import DeltaIOHandler

    __all__ = [*__all__, "DeltaIOHandler"]
except ImportError:
    pass

try:
    from rivers.io_handlers.iceberg import IcebergIOHandler

    __all__ = [*__all__, "IcebergIOHandler"]
except ImportError:
    pass
