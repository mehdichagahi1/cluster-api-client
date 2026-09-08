# Cluster API Client

An asynchronous Python client that creates and deletes a group across every node in a
cluster while handling transient HTTP failures and compensating for partial changes.

## Design

The server API has no transaction or idempotency-key endpoint, so strict distributed
atomicity cannot be guaranteed by a client alone. This implementation uses a
compensating-transaction (saga) approach:

1. Read every node with `GET` before making a change.
2. Return a successful no-op if every node is already in the requested state.
3. Refuse to mutate an initially inconsistent cluster, because there is no single safe
   baseline to restore.
4. Apply the change sequentially to make failure and rollback behavior deterministic.
5. Reconcile a timeout, connection error, or unexpected response with `GET`; the request
   may have reached the server even when its response did not reach the client.
6. Retry transient failures (`429` and `5xx`) with exponential backoff.
7. Verify the requested state on every node.
8. If any node fails, run the inverse operation on **every** node and verify that the
   original uniform state was restored.

Create rollback uses `DELETE` to restore absence. Delete rollback uses `POST` to restore
presence. Applying rollback to all nodes, rather than only nodes whose responses were
successful, also covers ambiguous timeouts.

An incomplete rollback raises `ClusterRollbackError` with the nodes that still require
operator attention. A failed operation whose rollback succeeds raises
`ClusterOperationError`.

### Assumptions

- A group is fully identified by `groupId`; recreating it does not lose other state.
- There are no concurrent writers changing the same `groupId` during an operation.
- `GET` becomes consistent within the configured retry window.
- A hostname without a scheme, as shown in the challenge, means HTTPS.
- `200` with exactly `{"groupId": "..."}` means present and `404` means absent.
- `201` is the successful create status and `200` is the successful delete status.
- A create `400` or delete error is accepted only when a follow-up `GET` proves that the
  desired state already exists. Other non-transient `4xx` responses fail immediately.
- Authentication and TLS material are environment-specific and therefore not invented by
  this client. Library users can supply a configured `httpx.AsyncClient` if needed.

If the process is terminated during the short interval between a mutation and rollback,
the cluster may remain inconsistent. Solving that failure mode requires server support
such as transactions/idempotency keys, or a durable workflow engine that can resume a
recorded operation.

## Requirements

- Python 3.11 or newer
- Docker (optional)
- Kubernetes and Kustomize support in `kubectl` (optional)

Runtime and development dependencies are pinned in `requirements.txt` and
`requirements-dev.txt` for repeatable builds.

## Install and test

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pip install --no-deps -e .
ruff check .
pytest --cov=cluster_api_client --cov-report=term-missing
```

## Library usage

```python
import asyncio

from cluster_api_client import ClusterClient

HOSTS = [
    "node1.example.com",
    "node2.example.com",
    "node3.example.com",
]


async def main() -> None:
    async with ClusterClient(HOSTS, timeout=5, max_attempts=3) as client:
        created = await client.create_group("example-group")
        print(created.as_dict())

        deleted = await client.delete_group("example-group")
        print(deleted.as_dict())


asyncio.run(main())
```

Supplying a custom authenticated HTTP client:

```python
import httpx

from cluster_api_client import ClusterClient

http_client = httpx.AsyncClient(headers={"Authorization": "Bearer TOKEN"})
client = ClusterClient(HOSTS, client=http_client)

# The caller owns and closes a supplied httpx client.
```

## Command-line usage

Hosts can be repeated on the command line:

```bash
cluster-api-client \
  --host node1.example.com \
  --host node2.example.com \
  --host node3.example.com \
  create example-group
```

Or supplied as a comma-separated environment variable:

```bash
export CLUSTER_HOSTS="node1.example.com,node2.example.com,node3.example.com"
cluster-api-client create example-group
cluster-api-client delete example-group
```

Optional settings:

| CLI option | Environment variable | Default |
|---|---|---:|
| `--timeout` | `CLUSTER_TIMEOUT` | `5` seconds |
| `--attempts` | `CLUSTER_MAX_ATTEMPTS` | `3` |
| `--backoff` | `CLUSTER_BACKOFF_BASE` | `0.2` seconds |

The command writes one JSON result to stdout and exits with `0` on success. It writes a
JSON error to stderr and exits with `1` on an operational or validation failure.

## Docker

Build and run the image:

```bash
docker build -t cluster-api-client:local .
docker run --rm \
  -e CLUSTER_HOSTS="https://node1.example.com,https://node2.example.com" \
  cluster-api-client:local create example-group
```

The image runs as an unprivileged user and uses a read-only-compatible application layout.

## Kubernetes

This client performs a finite operation and exits, so a Kubernetes `Job` is more
appropriate than a continuously running `Deployment`.

1. Update `manifests/configmap.yaml` with real cluster hosts.
2. Update the image in `manifests/job.yaml` to an image available to the cluster.
3. Change the example `groupId` and operation in `args` if required.
4. Apply the manifests:

```bash
kubectl apply -k manifests/
kubectl logs job/cluster-api-client-create
```

To run another operation, delete or rename the completed Job before applying it again.
The Job retry policy is safe with this client's uniform-state idempotency check.

## Project layout

```text
src/cluster_api_client/  client library and CLI
tests/                   unit tests using httpx.MockTransport
manifests/               ConfigMap, Job, and Kustomization
.github/workflows/       lint, test, coverage, and image-build CI
Dockerfile               reproducible executable image
```

The tests are unit tests; they model node state, transient failures, ambiguous timeouts,
successful rollback, failed rollback, idempotency, and inconsistent preflight state. No
cluster implementation or end-to-end environment is included, as requested by the
challenge.
