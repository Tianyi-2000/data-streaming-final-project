# The Kafka Connect sink path (GRPH-05)

An **alternative write path** for the same three graph topics: instead of `src/graph_loader.py`
consuming them with `python-arango`, ArangoDB's own Kafka Connect sink connector consumes them and
writes the collections itself.

It is an *alternative*, not a second implementation, because both paths consume **the same three
topics with the same document shapes**. That is what makes the comparison meaningful — same input,
same interface, two independent writers, and the counts have to match exactly.

The direct `python-arango` writer remains the **documented fallback** and satisfies GRPH-01 through
GRPH-04 on its own. See [`../GRAPH.md`](../GRAPH.md).

---

## The connector artifact

Approved at a blocking provenance checkpoint before it was downloaded or placed on any plugin path.

| | |
|---|---|
| Publisher | `arangodb` (GitHub **Organization**), Apache-2.0 |
| Source | https://github.com/arangodb/kafka-connect-arangodb |
| Coordinate | `com.arangodb:kafka-connect-arangodb:2.0.0` |
| Channel | Maven Central **only** |
| URL | https://repo1.maven.org/maven2/com/arangodb/kafka-connect-arangodb/2.0.0/kafka-connect-arangodb-2.0.0.jar |
| sha256 | `519afaf07aec1b3ff725542d5799229f5eec9417cdd0a8ed00cbab7799e7ccac` |
| Size | 9,491,570 bytes |
| Connector class | `com.arangodb.kafka.ArangoSinkConnector` |

**Mind the groupId.** A namesake project, `jaredpetersen/kafka-connect-arangodb`, exists on GitHub
and shares the artifact name. Only `com.arangodb` is ArangoDB's own. The POM's `<scm>` points back at
`github.com/arangodb/kafka-connect-arangodb`, which is what ties the Maven coordinate to the
publishing organisation rather than leaving the two separately asserted.

Two things worth knowing about the distribution:

- **GitHub releases carry no binary assets** — every tag is source-only, so Maven Central is the only
  binary channel. "Download from the release page and check the hash there" is not a route that
  exists here.
- **The JAR is a fat/shaded uber-JAR** (5,894 entries). Its three `compile`-scope dependencies —
  `arangodb-java-driver-shaded`, `jackson-serde-json`, `jackson-serde-vpack`, all `com.arangodb`
  7.24.0 — are already bundled; the POM simply was not flattened. So this is genuinely **one**
  artifact, not four. `connect-api` is correctly *absent*: the worker provides it.

### Fetching it

The JAR is not committed (9.5 MB binary; `plugins/*.jar` is gitignored). Fetch and verify:

```bash
curl -sSLo connect/plugins/kafka-connect-arangodb-2.0.0.jar \
  https://repo1.maven.org/maven2/com/arangodb/kafka-connect-arangodb/2.0.0/kafka-connect-arangodb-2.0.0.jar

shasum -a 256 connect/plugins/kafka-connect-arangodb-2.0.0.jar
# must print: 519afaf07aec1b3ff725542d5799229f5eec9417cdd0a8ed00cbab7799e7ccac
```

**A mismatch is a stop, not a retry.**

---

## Running it

```bash
# 1. the credential, from the environment -- never written into a config file
mkdir -p connect/secrets
printf 'password=%s\n' "${ARANGO_ROOT_PASSWORD:-streamingfraud}" > connect/secrets/arango.properties
chmod 600 connect/secrets/arango.properties

# 2. the connector writes a SECOND database; create it and its collections first
#    (the connector writes documents, it does not create databases or collections)
python3 - <<'PY'
import sys; sys.path.insert(0,'.')
from arango import ArangoClient
from src.graph_loader import DEFAULT_ENDPOINT, ensure_graph
ensure_graph(ArangoClient(hosts=DEFAULT_ENDPOINT),
             database="streaming_fraud_graph_connect", drop=True)
PY

# 3. start the worker (same --profile graph as ArangoDB)
docker compose --profile graph up -d connect
curl -sf http://localhost:8083/connector-plugins | grep -o 'com.arangodb.kafka.ArangoSinkConnector'

# 4. register the three sinks
for f in connect/graph-*-sink.json; do
  curl -sS -X POST -H 'Content-Type: application/json' --data @"$f" \
    http://localhost:8083/connectors; echo
done

# 5. status is NOT proof -- compare counts (see GRAPH.md)
curl -sf http://localhost:8083/connectors/graph-played-sink/status
```

---

## The three configurations

One per topic, because each topic maps to a different collection. Property names were read out of the
connector's own `ArangoSinkConfig` class rather than transcribed from memory.

| Property | Value | Why |
|---|---|---|
| `connector.class` | `com.arangodb.kafka.ArangoSinkConnector` | the approved artifact's sink class |
| `topics` | one of the three graph topics | the interface both write paths share |
| `connection.endpoints` | `arangodb:8529` | **container to container.** The internal port, not the host's 8531 |
| `connection.database` | `streaming_fraud_graph_connect` | a **second** database — never the one the direct writer owns |
| `connection.collection` | `listeners` / `tracks` / `played` | `played` is the edge collection; `_from`/`_to` ride in the document |
| `insert.overwriteMode` | `replace` | the connector-side counterpart of the direct writer's `overwrite_mode="replace"`, and the REPSERT semantic GRPH-04 exists to reinstate |
| `data.errors.tolerance` | `none` | fail rather than silently skip a bad record — the same posture as the loader's `raise_on_document_error=True` |
| `key.converter` | `StringConverter` | the Kafka keys are UTF-8 ids, matching each document's `_key` |
| `value.converter` | `JsonConverter`, `schemas.enable=false` | the emitter produces plain JSON with no schema envelope |
| `connection.password` | `${file:...}` | resolved by `FileConfigProvider` at run time, so **no credential is written into these files** |

The three files differ in exactly three lines — name, topic, collection:

```
$ diff connect/graph-listeners-sink.json connect/graph-played-sink.json
2c2   "name": "graph-listeners-sink"      ->  "graph-played-sink"
6c6   "topics": "graph-listeners"         ->  "graph-played"
12c12 "connection.collection": "listeners" ->  "played"
```

### Two ways this fails silently, and what stops each

- **`localhost:9092` instead of `redpanda:29092`.** The worker runs *inside* the compose network, so
  it must use the internal listener. Point it at `localhost` and the worker starts, reports
  `RUNNING`, and consumes nothing forever. `docker-compose.yml` carries the reason next to the value.
- **`RUNNING` mistaken for "it worked".** A misconfigured sink reports `RUNNING` while writing
  nothing. That is why acceptance here is a **count comparison against the direct writer**, never a
  status check.
