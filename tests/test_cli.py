from __future__ import annotations

import argparse

import pytest

from cluster_api_client import OperationResult
from cluster_api_client import cli as cli_module
from cluster_api_client.cli import _resolve_hosts, _run, build_parser, main


def test_cli_accepts_repeated_hosts() -> None:
    args = build_parser().parse_args(
        ["--host", "node1.example.com", "--host", "node2.example.com", "create", "g-1"]
    )

    assert args.hosts == ["node1.example.com", "node2.example.com"]
    assert args.operation == "create"
    assert args.group_id == "g-1"


def test_hosts_fall_back_to_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLUSTER_HOSTS", "node1.example.com, node2.example.com")

    assert _resolve_hosts(None) == ["node1.example.com", "node2.example.com"]


def test_missing_hosts_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CLUSTER_HOSTS", raising=False)

    with pytest.raises(ValueError, match="CLUSTER_HOSTS"):
        _resolve_hosts(None)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["create", "delete"])
async def test_run_dispatches_operation(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    calls: list[str] = []

    class StubClient:
        def __init__(self, hosts: list[str], **_: object) -> None:
            assert hosts == ["node1.example.com"]

        async def __aenter__(self) -> StubClient:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def create_group(self, group_id: str) -> OperationResult:
            calls.append(f"create:{group_id}")
            return OperationResult("create", group_id, ("https://node1.example.com",), True)

        async def delete_group(self, group_id: str) -> OperationResult:
            calls.append(f"delete:{group_id}")
            return OperationResult("delete", group_id, ("https://node1.example.com",), True)

    monkeypatch.setattr(cli_module, "ClusterClient", StubClient)
    args = argparse.Namespace(
        hosts=["node1.example.com"],
        timeout=5,
        attempts=3,
        backoff=0,
        operation=operation,
        group_id="g-1",
    )

    result = await _run(args)

    assert calls == [f"{operation}:g-1"]
    assert result["status"] == "ok"


def test_main_prints_json_success(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def fake_run(_: argparse.Namespace) -> dict[str, object]:
        return {"status": "ok", "changed": True}

    monkeypatch.setattr(cli_module, "_run", fake_run)

    assert main(["--host", "node1.example.com", "create", "g-1"]) == 0
    assert '"status": "ok"' in capsys.readouterr().out


def test_main_prints_json_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def fake_run(_: argparse.Namespace) -> dict[str, object]:
        raise ValueError("bad configuration")

    monkeypatch.setattr(cli_module, "_run", fake_run)

    assert main(["--host", "node1.example.com", "delete", "g-1"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert '"error": "ValueError"' in captured.err
