from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable

import httpx
import pytest

from cluster_api_client import (
    ClusterClient,
    ClusterOperationError,
    ClusterRollbackError,
    InconsistentClusterState,
    NodeRequestError,
    OperationResult,
)

HOSTS = ("node1.example.com", "node2.example.com", "node3.example.com")


class FakeCluster:
    """Stateful in-memory implementation of the documented HTTP API."""

    def __init__(
        self,
        *,
        present_on: Iterable[str] = (),
        scripts: dict[tuple[str, str], list[int | str]] | None = None,
    ) -> None:
        present = set(present_on)
        self.state = {host: host in present for host in HOSTS}
        self.scripts = {key: list(actions) for key, actions in (scripts or {}).items()}
        self.calls: list[tuple[str, str]] = []
        self.counts: defaultdict[tuple[str, str], int] = defaultdict(int)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        assert host is not None
        key = (request.method, host)
        self.calls.append(key)
        self.counts[key] += 1

        action = self.scripts.get(key, []).pop(0) if self.scripts.get(key) else None
        if action == "timeout":
            raise httpx.ConnectTimeout("scripted timeout", request=request)
        if action == "apply_then_timeout":
            self._apply(request, host)
            raise httpx.ReadTimeout("response was lost", request=request)
        if isinstance(action, int):
            return httpx.Response(action, request=request)

        if request.method == "GET":
            if self.state[host]:
                group_id = request.url.path.split("/")[-2]
                return httpx.Response(200, json={"groupId": group_id}, request=request)
            return httpx.Response(404, request=request)
        return self._apply(request, host)

    def _apply(self, request: httpx.Request, host: str) -> httpx.Response:
        payload = json.loads(request.content)
        assert payload["groupId"]
        if request.method == "POST":
            if self.state[host]:
                return httpx.Response(400, request=request)
            self.state[host] = True
            return httpx.Response(201, request=request)
        if request.method == "DELETE":
            if not self.state[host]:
                return httpx.Response(404, request=request)
            self.state[host] = False
            return httpx.Response(200, request=request)
        raise AssertionError(f"unexpected method {request.method}")


def make_client(fake: FakeCluster, *, max_attempts: int = 2) -> ClusterClient:
    return ClusterClient(
        HOSTS,
        max_attempts=max_attempts,
        backoff_base=0,
        transport=httpx.MockTransport(fake),
    )


@pytest.mark.asyncio
async def test_create_group_on_every_node_and_verify() -> None:
    fake = FakeCluster()

    async with make_client(fake) as client:
        result = await client.create_group("team-a")

    assert result.changed is True
    assert result.operation == "create"
    assert all(fake.state.values())
    for host in HOSTS:
        assert fake.counts[("POST", host)] == 1
        assert fake.counts[("GET", host)] >= 2  # preflight and final verification


@pytest.mark.asyncio
async def test_uniform_target_state_is_an_idempotent_noop() -> None:
    fake = FakeCluster(present_on=HOSTS)

    async with make_client(fake) as client:
        result = await client.create_group("team-a")

    assert result.changed is False
    assert not any(method == "POST" for method, _ in fake.calls)


@pytest.mark.asyncio
async def test_inconsistent_preflight_state_causes_no_mutation() -> None:
    fake = FakeCluster(present_on=[HOSTS[0]])

    async with make_client(fake) as client:
        with pytest.raises(InconsistentClusterState):
            await client.create_group("team-a")

    assert not any(method in {"POST", "DELETE"} for method, _ in fake.calls)


@pytest.mark.asyncio
async def test_timeout_is_reconciled_when_server_applied_the_create() -> None:
    fake = FakeCluster(
        scripts={("POST", HOSTS[0]): ["apply_then_timeout"]},
    )

    async with make_client(fake) as client:
        result = await client.create_group("team-a")

    assert result.changed is True
    assert all(fake.state.values())
    assert fake.counts[("POST", HOSTS[0])] == 1


@pytest.mark.asyncio
async def test_create_failure_rolls_back_every_node_to_absent() -> None:
    fake = FakeCluster(scripts={("POST", HOSTS[1]): [500]})

    async with make_client(fake, max_attempts=1) as client:
        with pytest.raises(ClusterOperationError) as raised:
            await client.create_group("team-a")

    assert not isinstance(raised.value, ClusterRollbackError)
    assert not any(fake.state.values())
    for host in HOSTS:
        assert fake.counts[("DELETE", host)] == 1


@pytest.mark.asyncio
async def test_delete_failure_recreates_group_on_every_node() -> None:
    fake = FakeCluster(
        present_on=HOSTS,
        scripts={("DELETE", HOSTS[1]): [500]},
    )

    async with make_client(fake, max_attempts=1) as client:
        with pytest.raises(ClusterOperationError):
            await client.delete_group("team-a")

    assert all(fake.state.values())
    for host in HOSTS:
        assert fake.counts[("POST", host)] == 1


@pytest.mark.asyncio
async def test_incomplete_rollback_is_reported_separately() -> None:
    fake = FakeCluster(
        scripts={
            ("POST", HOSTS[1]): [500],
            ("DELETE", HOSTS[0]): [500],
        },
    )

    async with make_client(fake, max_attempts=1) as client:
        with pytest.raises(ClusterRollbackError) as raised:
            await client.create_group("team-a")

    assert raised.value.rollback_failures[0].host == f"https://{HOSTS[0]}"
    assert fake.state[HOSTS[0]] is True


@pytest.mark.asyncio
async def test_transient_get_is_retried() -> None:
    fake = FakeCluster(scripts={("GET", HOSTS[0]): [503]})

    async with make_client(fake) as client:
        result = await client.create_group("team-a")

    assert result.changed is True
    assert fake.counts[("GET", HOSTS[0])] >= 3


@pytest.mark.asyncio
async def test_unexpected_get_payload_is_a_protocol_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"groupId": "another-group"}, request=request)

    client = ClusterClient(
        [HOSTS[0]],
        transport=httpx.MockTransport(handler),
        backoff_base=0,
    )
    async with client:
        with pytest.raises(NodeRequestError, match="unexpected response body"):
            await client.create_group("team-a")


@pytest.mark.asyncio
async def test_non_retryable_create_error_is_rolled_back() -> None:
    fake = FakeCluster(scripts={("POST", HOSTS[0]): [400]})

    async with make_client(fake) as client:
        with pytest.raises(ClusterOperationError):
            await client.create_group("team-a")

    assert not any(fake.state.values())


@pytest.mark.asyncio
async def test_failed_final_verification_triggers_rollback() -> None:
    fake = FakeCluster(scripts={("GET", HOSTS[0]): [404, 404, 404]})

    async with make_client(fake) as client:
        with pytest.raises(ClusterOperationError, match="initial cluster state was restored"):
            await client.create_group("team-a")

    assert not any(fake.state.values())


def test_hosts_are_normalized_and_deduplicated() -> None:
    client = ClusterClient(
        ["node1.example.com/", "https://node1.example.com"],
        transport=httpx.MockTransport(lambda request: httpx.Response(404, request=request)),
    )

    assert client.hosts == ("https://node1.example.com",)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"hosts": []}, "at least one host"),
        ({"hosts": [""]}, "non-empty strings"),
        ({"hosts": ["ftp://node.example.com"]}, "invalid host URL"),
        ({"hosts": ["https://node.example.com?q=1"]}, "query or fragment"),
        ({"hosts": HOSTS, "timeout": 0}, "timeout"),
        ({"hosts": HOSTS, "max_attempts": 0}, "max_attempts"),
        ({"hosts": HOSTS, "backoff_base": -1}, "backoff_base"),
    ],
)
def test_invalid_configuration_is_rejected(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        ClusterClient(**kwargs)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_empty_group_id_is_rejected() -> None:
    fake = FakeCluster()

    async with make_client(fake) as client:
        with pytest.raises(ValueError, match="group_id"):
            await client.create_group(" ")


def test_operation_result_serializes_for_cli() -> None:
    result = OperationResult("create", "g-1", ("https://node1.example.com",), True)

    assert result.as_dict() == {
        "operation": "create",
        "groupId": "g-1",
        "hosts": ["https://node1.example.com"],
        "changed": True,
        "status": "ok",
    }
