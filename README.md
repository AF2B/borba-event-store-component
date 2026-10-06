# borba-event-store-component

[![CI](https://github.com/AF2B/borba-event-store-component/actions/workflows/ci.yml/badge.svg)](https://github.com/AF2B/borba-event-store-component/actions/workflows/ci.yml)

An append-only event store on PostgreSQL for a Borba service that keeps what happened to an aggregate and not only where it is now.
Events are appended at a version, so two commands that read the same history cannot both win; the table cannot be rewritten, not
even by a mistake; and the payload comes back as data.

## Install

```clojure
io.github.af2b/borba-event-store-component
{:git/url "https://github.com/AF2B/borba-event-store-component"
 :git/tag "v2.0.0"
 :git/sha "<the commit of the tag, printed in the release notes>"}
```

It depends on Clojure, Integrant, `tools.logging`, [jsonista](https://github.com/metosin/jsonista) and
`borba-sql-client-component`, whose database it uses.

## Use

```clojure
{:service/namespaces [borba.sql-client borba.event-store]

 :ig/system
 {:components/database    {:jdbc-url #env DATABASE_URL
                           :username #env DATABASE_USER
                           :password #env DATABASE_PASSWORD}
  :components/event-store {:database #ig/ref :components/database}}}
```

The component makes the table when it starts, if there is none, once at a time, so several instances of a service that start together
do not trip over each other. The value is the store that every function takes first.

### Appending

An event is a map of the aggregate it belongs to, what it is, what happened and its place in the history of the aggregate:

```clojure
(require '[borba.event-store :as es])

(es/append! store {:aggregate-id   order-id
                   :aggregate-type "order"
                   :event-type     "order.created"
                   :payload        {:total 10 :items [{:sku "a" :qty 2}]}
                   :version        1
                   :metadata       {:by "ana"}})
;; => {:position       41
;;     :id             #uuid "…"
;;     :aggregate-id   "…"
;;     :aggregate-type "order"
;;     :event-type     "order.created"
;;     :payload        {:total 10, :items [{:sku "a", :qty 2}]}
;;     :metadata       {:by "ana"}
;;     :version        1
;;     :created-at     #inst "2026-10-06T21:20:11.5Z"}
```

`:aggregate-id` is a string or a UUID, `:payload` is a map, `:version` is a positive integer and `:metadata` is a map, or left out. An
event that is not valid is a mistake of the program: it throws, saying everything that is wrong at once.

### Versions, and the conflict

The first event of an aggregate is version 1, and each one after is **one more than the one before**. That is the optimistic
concurrency of an event store. When two commands read the history at version 4 and both append version 5, the first wins and the second
is told, as data, so it can read the history again and decide:

```clojure
(es/append! store (assoc event :version 3))     ;; when the aggregate is at version 1
;; => {:error            :version-conflict
;;     :aggregate-id     "…"
;;     :version          3
;;     :expected-version 2
;;     :actual-version   1}
```

Nothing is stored for a conflict. It is the same answer for a version that is taken and for one that skips ahead, and it holds when the
commands are in different threads or different processes: of eight that append version 1 at the same time, one is stored and seven get
the conflict, which the integration suite runs. The check is made by the database, in the statement that stores the event, and the
unique constraint on the aggregate and its version is the net under it.

### Reading

```clojure
(es/get-events store order-id)                         ;; all, in the order of the versions
(es/get-events store order-id {:from-version 4})       ;; from a version
(es/get-latest-version store order-id)                 ;; 0 when there are none

(es/get-events-by-type store "order.created" {:limit 100})   ;; in the order they were stored in
(es/get-all-events store {:after 4100 :limit 500})           ;; every aggregate: the feed of a projection
```

The events of a type and the feed are in the order of their `:position`, which is the order they were stored in, across aggregates, and
they are never more than `:limit` (1000 unless told otherwise, 10000 at most). To read on, ask again with the `:position` of the last
event as `:after`. A limit or a position that is not valid throws before the query is sent.

`get-aggregate-snapshot` folds the events of an aggregate, in the order of their versions, into its state:

```clojure
(es/get-aggregate-snapshot store order-id
                           (fn [state event]
                             (case (:event-type event)
                               "order.created" (merge state (:payload event))
                               "order.shipped" (assoc state :status :shipped)
                               state)))
```

### Payloads

The payload and the metadata are JSONB. They are written as the data is and read back with the keys as keywords, so a map goes in and
comes out the same, a namespaced key such as `:order/id` included. What JSON has no type for does not come back as it went: a keyword
value is a string, and a set is a vector.

## Append-only

The table has triggers that refuse an `UPDATE`, a `DELETE` and a `TRUNCATE`, with the SQLSTATE `23001`, so the history cannot be rewritten
by the service, by a script or by a mistake. A correction is a new event.

```sql
UPDATE events SET event_type = 'x' WHERE aggregate_id = 'agg-1';
-- ERROR:  the events are append-only: UPDATE is not allowed
```

The triggers are made with the table. A table that was made by an earlier version of this library, with the same name and without
`position`, has to be migrated, or renamed and copied into the new one: `CREATE TABLE IF NOT EXISTS` does not change a table that
exists.

## What is stored

```sql
CREATE TABLE events (
  position       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id             UUID NOT NULL UNIQUE,
  aggregate_id   VARCHAR(255) NOT NULL,
  aggregate_type VARCHAR(255) NOT NULL,
  event_type     VARCHAR(255) NOT NULL,
  payload        JSONB NOT NULL,
  metadata       JSONB NOT NULL DEFAULT '{}',
  version        BIGINT NOT NULL CHECK (version > 0),
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT events_aggregate_version_key UNIQUE (aggregate_id, version)
);
```

The `created_at` is the time of the database, so it does not depend on the clock of the service that appends. `position` is what gives the
events a total order, which a timestamp does not: two events can have the same one.

## API

| Name | What it does |
|---|---|
| `append!` | Appends an event at its version, or says there is a conflict |
| `get-events` | The events of an aggregate, from a version |
| `get-latest-version` | The version of the last event of an aggregate |
| `get-events-by-type` | The events of a type, a page at a time |
| `get-all-events` | The events of every aggregate, a page at a time |
| `get-aggregate-snapshot` | Folds the events of an aggregate into its state |
| `:components/event-store` | The Integrant key that makes the table and returns the store |

## Tests

The unit suite runs anywhere; it covers the checks of an event, the codec of the payload and the limits. The integration suite runs
against a real PostgreSQL, which the pipeline provides, and which you can start with Docker:

```bash
docker run --rm -d --name borba-sql-it -p 127.0.0.1:55432:5432 \
  -e POSTGRES_USER=ci -e POSTGRES_PASSWORD=ci -e POSTGRES_DB=ci postgres:18.6-alpine

DATABASE_URL=jdbc:postgresql://127.0.0.1:55432/ci DATABASE_USER=ci DATABASE_PASSWORD=ci \
  make test-integration
```

It covers appending and reading, the versions and the conflict, eight commands that append the same version at once, the paging,
the payloads, the triggers, and the schema made by several starts at the same time. The coverage the pipeline measures is the one of
the unit suite, which is why its floor is low.

## Design notes

- **The database decides the conflict.** The check that the version follows the last one is in the `INSERT`, not in a read before it,
  because a read before it is the race that optimistic concurrency exists to avoid.
- **An expected failure is data.** A conflict is not an exceptional thing: it is what the history says when two commands meet, and the
  caller has something to do about it. A mistake of the program, an event that is not valid, throws.
- **A read is bounded.** The events of a type and the feed come in pages, with a position to read on from, so a table of millions of
  events is not an answer.
- **Append-only is a property of the table.** A rule in the code can be bypassed by the code, so the database holds it.

## Development

```bash
make check      # lint, format, conventions, reflection, tests, coverage
make ci         # everything the pipelines enforce
```

See [CONTRIBUTING.md](CONTRIBUTING.md). The repository follows the [Borba standard](https://github.com/AF2B/borba-tooling/blob/main/docs/standard.md).

## License

[MIT](LICENSE)
