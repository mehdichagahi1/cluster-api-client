"""Command-line interface for the cluster client."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Sequence

from .client import ClusterClient
from .errors import ClusterClientError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cluster-api-client",
        description="Reliably create or delete a group across cluster nodes.",
    )
    parser.add_argument(
        "--host",
        action="append",
        dest="hosts",
        help=(
            "cluster node base URL; repeat for each node. If omitted, use the "
            "comma-separated CLUSTER_HOSTS environment variable"
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(os.getenv("CLUSTER_TIMEOUT", "5")),
        help="per-request timeout in seconds (default: 5)",
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=int(os.getenv("CLUSTER_MAX_ATTEMPTS", "3")),
        help="maximum request attempts per node (default: 3)",
    )
    parser.add_argument(
        "--backoff",
        type=float,
        default=float(os.getenv("CLUSTER_BACKOFF_BASE", "0.2")),
        help="initial exponential backoff in seconds (default: 0.2)",
    )

    subparsers = parser.add_subparsers(dest="operation", required=True)
    for operation in ("create", "delete"):
        command = subparsers.add_parser(operation, help=f"{operation} a group")
        command.add_argument("group_id", help="groupId sent to every cluster node")
    return parser


def _resolve_hosts(cli_hosts: list[str] | None) -> list[str]:
    if cli_hosts:
        return cli_hosts
    env_hosts = os.getenv("CLUSTER_HOSTS", "")
    hosts = [host.strip() for host in env_hosts.split(",") if host.strip()]
    if not hosts:
        raise ValueError("provide --host at least once or set CLUSTER_HOSTS")
    return hosts


async def _run(args: argparse.Namespace) -> dict[str, object]:
    hosts = _resolve_hosts(args.hosts)
    async with ClusterClient(
        hosts,
        timeout=args.timeout,
        max_attempts=args.attempts,
        backoff_base=args.backoff,
    ) as client:
        if args.operation == "create":
            result = await client.create_group(args.group_id)
        else:
            result = await client.delete_group(args.group_id)
    return result.as_dict()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = asyncio.run(_run(args))
    except (ClusterClientError, ValueError) as error:
        print(
            json.dumps(
                {"status": "error", "error": type(error).__name__, "detail": str(error)},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
