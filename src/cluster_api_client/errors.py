"""Exception types exposed by the cluster client."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


class ClusterClientError(RuntimeError):
    """Base class for errors raised by this package."""


@dataclass(frozen=True, slots=True)
class NodeFailure:
    """A failure encountered while communicating with one cluster node."""

    host: str
    phase: str
    detail: str


class NodeRequestError(ClusterClientError):
    """A node did not reach or report the expected state."""

    def __init__(self, failure: NodeFailure) -> None:
        self.failure = failure
        super().__init__(f"node {failure.host!r} failed during {failure.phase}: {failure.detail}")


class InconsistentClusterState(ClusterClientError):
    """Nodes disagreed about whether a group existed before an operation."""

    def __init__(self, group_id: str, states: Mapping[str, bool]) -> None:
        self.group_id = group_id
        self.states = dict(states)
        rendered = ", ".join(
            f"{host}={'present' if present else 'absent'}" for host, present in self.states.items()
        )
        super().__init__(f"group {group_id!r} has an inconsistent initial state: {rendered}")


class ClusterOperationError(ClusterClientError):
    """A create/delete operation failed but rollback completed."""

    def __init__(
        self,
        operation: str,
        group_id: str,
        cause: NodeRequestError,
    ) -> None:
        self.operation = operation
        self.group_id = group_id
        self.cause = cause
        super().__init__(
            f"{operation} of group {group_id!r} failed; the initial cluster "
            f"state was restored: {cause}"
        )


class ClusterRollbackError(ClusterOperationError):
    """An operation failed and one or more rollback actions also failed."""

    def __init__(
        self,
        operation: str,
        group_id: str,
        cause: NodeRequestError,
        rollback_failures: tuple[NodeFailure, ...],
    ) -> None:
        self.rollback_failures = rollback_failures
        super().__init__(operation, group_id, cause)
        rollback_summary = "; ".join(
            f"{failure.host}: {failure.detail}" for failure in rollback_failures
        )
        self.args = (
            f"{operation} of group {group_id!r} failed and rollback was "
            f"incomplete ({rollback_summary}); original error: {cause}",
        )
