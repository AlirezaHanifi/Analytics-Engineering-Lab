from collections.abc import Sequence
from typing import Any


class ClickHouseManager:
    """Wrapper for managing ClickHouse connections and query execution."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._client = None

    @property
    def client(self) -> Any:
        """Lazy instantiation of the ClickHouse client."""
        if self._client is None:
            from clickhouse_driver import Client

            self._client = Client(host=self.host, port=self.port)
        return self._client

    def execute_query(self, query: str, params: Sequence[tuple] | None = None) -> Any:
        """Executes a query, optionally taking a list of tuples for bulk inserts."""
        if params:
            return self.client.execute(query, params)
        return self.client.execute(query)
