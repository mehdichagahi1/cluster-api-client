"""Reliable, compensating client for the cluster group API."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Literal
from urllib.parse import quote, urlsplit

import httpx

from .errors import (
    ClusterOperationError,
    ClusterRollbackError,
    InconsistentClusterState,
    NodeFailure,
    NodeRequestError,
)

Operation = Literal["create", "delete"]
Sleeper = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class OperationResult:
    """Summary of a successful cluster-wide operation."""

    operation: Operation
    group_id: str
    hosts: tuple[str, ...]
    changed: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "operation": self.operation,
            "groupId": self.group_id,
            "hosts": list(self.hosts),
            "changed": self.changed,
            "status": "ok",
        }


class ClusterClient:
    """Create and delete groups consistently across a fixed set of nodes.

    The remote API offers no transaction primitive, so this client uses a
    saga-like compensating operation and verifies state with GET requests.
    Operations are deliberately sequential to make rollback deterministic.
    """

    _TRANSIENT_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

    def __init__(
        self,
        hosts: Iterable[str],
        *,
        timeout: float = 5.0,
        max_attempts: int = 3,
        backoff_base: float = 0.2,
        client: httpx.AsyncClient | None = None,
        transport: httpx.AsyncBaseTransport | httpx.BaseTransport | None = None,
        sleeper: Sleeper = asyncio.sleep,
    ) -> None:
        normalized_hosts = tuple(dict.fromkeys(self._normalize_host(host) for host in hosts))
        if not normalized_hosts:
            raise ValueError("at least one host is required")
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        if backoff_base < 0:
            raise ValueError("backoff_base cannot be negative")
        if client is not None and transport is not None:
            raise ValueError("pass either client or transport, not both")

        self.hosts = normalized_hosts
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self._sleeper = sleeper
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            transport=transport,
            headers={"Accept": "application/json"},
        )

    async def __aenter__(self) -> ClusterClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the internally-created HTTP client."""

        if self._owns_client:
            await self._client.aclose()

    async def create_group(self, group_id: str) -> OperationResult:
        """Ensure the group exists on every node, or restore absence."""

        return await self._change_group(group_id, desired_present=True)

    async def delete_group(self, group_id: str) -> OperationResult:
        """Ensure the group is absent on every node, or restore presence."""

        return await self._change_group(group_id, desired_present=False)

    async def _change_group(
        self,
        group_id: str,
        *,
        desired_present: bool,
    ) -> OperationResult:
        self._validate_group_id(group_id)
        operation: Operation = "create" if desired_present else "delete"

        initial_states: dict[str, bool] = {}
        for host in self.hosts:
            initial_states[host] = await self._read_state(
                host,
                group_id,
                phase="preflight",
            )

        unique_states = set(initial_states.values())
        if len(unique_states) != 1:
            raise InconsistentClusterState(group_id, initial_states)

        initial_present = next(iter(unique_states))
        if initial_present == desired_present:
            return OperationResult(operation, group_id, self.hosts, changed=False)

        try:
            for host in self.hosts:
                await self._ensure_state(
                    host,
                    group_id,
                    present=desired_present,
                    phase=operation,
                )

            for host in self.hosts:
                await self._wait_for_state(
                    host,
                    group_id,
                    present=desired_present,
                    phase=f"verify-{operation}",
                )
        except NodeRequestError as cause:
            rollback_failures = await self._restore_state(
                group_id,
                present=initial_present,
            )
            if rollback_failures:
                raise ClusterRollbackError(
                    operation,
                    group_id,
                    cause,
                    rollback_failures,
                ) from cause
            raise ClusterOperationError(operation, group_id, cause) from cause

        return OperationResult(operation, group_id, self.hosts, changed=True)

    async def _restore_state(
        self,
        group_id: str,
        *,
        present: bool,
    ) -> tuple[NodeFailure, ...]:
        failures: list[NodeFailure] = []
        for host in self.hosts:
            try:
                await self._ensure_state(
                    host,
                    group_id,
                    present=present,
                    phase="rollback",
                )
                await self._wait_for_state(
                    host,
                    group_id,
                    present=present,
                    phase="verify-rollback",
                )
            except NodeRequestError as error:
                failures.append(error.failure)
        return tuple(failures)

    async def _ensure_state(
        self,
        host: str,
        group_id: str,
        *,
        present: bool,
        phase: str,
    ) -> None:
        method = "POST" if present else "DELETE"
        expected_status = 201 if present else 200
        url = f"{host}/v1/group/"
        last_detail = "no request was attempted"

        for attempt in range(1, self.max_attempts + 1):
            response: httpx.Response | None = None
            try:
                response = await self._client.request(
                    method,
                    url,
                    json={"groupId": group_id},
                )
                if response.status_code == expected_status:
                    return
                last_detail = f"{method} returned HTTP {response.status_code}"
            except httpx.RequestError as error:
                last_detail = f"{method} raised {type(error).__name__}: {error}"

            # A timeout or error response is ambiguous: the server may have
            # applied the request before the response was lost. Reconcile via
            # the documented GET endpoint before deciding to retry.
            try:
                if (
                    await self._read_state(
                        host,
                        group_id,
                        phase=f"reconcile-{phase}",
                    )
                    == present
                ):
                    return
            except NodeRequestError as error:
                last_detail = f"{last_detail}; reconciliation failed: {error.failure.detail}"

            if response is not None and not self._is_retryable(response.status_code):
                break
            if attempt < self.max_attempts:
                await self._pause(attempt)

        raise NodeRequestError(NodeFailure(host, phase, last_detail))

    async def _read_state(self, host: str, group_id: str, *, phase: str) -> bool:
        encoded_group_id = quote(group_id, safe="")
        url = f"{host}/v1/group/{encoded_group_id}/"
        last_detail = "GET was not attempted"

        for attempt in range(1, self.max_attempts + 1):
            try:
                response = await self._client.get(url)
            except httpx.RequestError as error:
                last_detail = f"GET raised {type(error).__name__}: {error}"
            else:
                if response.status_code == 404:
                    return False
                if response.status_code == 200:
                    try:
                        payload = response.json()
                    except ValueError as error:
                        raise NodeRequestError(
                            NodeFailure(host, phase, f"GET returned invalid JSON: {error}")
                        ) from error
                    if payload == {"groupId": group_id}:
                        return True
                    raise NodeRequestError(
                        NodeFailure(
                            host,
                            phase,
                            "GET returned an unexpected response body",
                        )
                    )

                last_detail = f"GET returned HTTP {response.status_code}"
                if not self._is_retryable(response.status_code):
                    break

            if attempt < self.max_attempts:
                await self._pause(attempt)

        raise NodeRequestError(NodeFailure(host, phase, last_detail))

    async def _wait_for_state(
        self,
        host: str,
        group_id: str,
        *,
        present: bool,
        phase: str,
    ) -> None:
        last_observed: bool | None = None
        for attempt in range(1, self.max_attempts + 1):
            last_observed = await self._read_state(host, group_id, phase=phase)
            if last_observed == present:
                return
            if attempt < self.max_attempts:
                await self._pause(attempt)

        expected = "present" if present else "absent"
        observed = "present" if last_observed else "absent"
        raise NodeRequestError(
            NodeFailure(
                host,
                phase,
                f"expected group to be {expected}, observed {observed}",
            )
        )

    async def _pause(self, attempt: int) -> None:
        await self._sleeper(self.backoff_base * (2 ** (attempt - 1)))

    @classmethod
    def _is_retryable(cls, status_code: int) -> bool:
        return status_code in cls._TRANSIENT_STATUS_CODES or status_code >= 500

    @staticmethod
    def _normalize_host(host: str) -> str:
        if not isinstance(host, str) or not host.strip():
            raise ValueError("hosts must be non-empty strings")
        normalized = host.strip()
        if "://" not in normalized:
            normalized = f"https://{normalized}"
        parsed = urlsplit(normalized)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(f"invalid host URL: {host!r}")
        if parsed.query or parsed.fragment:
            raise ValueError(f"host URL cannot contain a query or fragment: {host!r}")
        return normalized.rstrip("/")

    @staticmethod
    def _validate_group_id(group_id: str) -> None:
        if not isinstance(group_id, str) or not group_id.strip():
            raise ValueError("group_id must be a non-empty string")
