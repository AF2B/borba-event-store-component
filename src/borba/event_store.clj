(ns borba.event-store
  "An append-only event store on PostgreSQL, for a service that keeps what
   happened to an aggregate and not only where it is now.

     :components/event-store {:database #ig/ref :components/database}

   The database is the one of borba-sql-client-component. The component makes
   the table when it starts, if there is none, and makes it append-only: a
   trigger refuses an UPDATE, a DELETE and a TRUNCATE, so the history cannot be
   rewritten by the program, a script or a mistake.

   An event is appended at a version: the first of an aggregate is 1, and each
   one after is one more than the one before. That is the optimistic
   concurrency of an event store. Two commands that read the history at version
   4 and both append version 5 cannot both win: the second gets a
   :version-conflict, as data, and reads the history again.

   Events come back as maps with kebab-case keys, with the payload and the
   metadata as data and the time as an Instant, in the order of their
   :position, which is the order they were stored in, across aggregates."
  (:require
   [borba.event-store.codec :as codec]
   [borba.event-store.event :as event]
   [borba.sql-client :as sql]
   [clojure.tools.logging :as log]
   [integrant.core :as ig]))

(set! *warn-on-reflection* true)

(def default-limit
  "How many events a read of the events of a type, or of all of them, returns
   unless told otherwise."
  1000)

(def max-limit
  "The most events a read can ask for at once."
  10000)

(def ^:private schema-lock-id
  "The advisory lock that keeps two services that start together from making
   the table at the same time, which PostgreSQL does not do cleanly."
  5738243791)

(def ^:private unique-constraint "events_aggregate_version_key")

(def ^:private schema-statements
  ["CREATE TABLE IF NOT EXISTS events (
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
    )"
   "CREATE INDEX IF NOT EXISTS events_event_type_position_idx
      ON events (event_type, position)"
   "CREATE OR REPLACE FUNCTION events_are_append_only() RETURNS trigger AS $$
    BEGIN
      RAISE EXCEPTION 'the events are append-only: % is not allowed', TG_OP
        USING ERRCODE = 'restrict_violation';
    END;
    $$ LANGUAGE plpgsql"
   "CREATE OR REPLACE TRIGGER events_no_update_or_delete
      BEFORE UPDATE OR DELETE ON events
      FOR EACH ROW EXECUTE FUNCTION events_are_append_only()"
   "CREATE OR REPLACE TRIGGER events_no_truncate
      BEFORE TRUNCATE ON events
      FOR EACH STATEMENT EXECUTE FUNCTION events_are_append_only()"])

(defn- ensure-schema!
  "Makes the table, its index and the triggers that keep it append-only, once
   at a time."
  [datasource]
  (sql/transact! datasource
                 (fn [tx]
                   (sql/execute! tx ["SELECT pg_advisory_xact_lock(?)"
                                     schema-lock-id])
                   (doseq [statement schema-statements]
                     (sql/execute! tx [statement])))))

(defmethod ig/init-key :components/event-store
  [_ {:keys [database]}]
  (when-not database
    (throw (ex-info ":database is the data source of :components/database"
                    {:error ::no-database})))
  (ensure-schema! database)
  (log/info "event store ready")
  {:datasource database})

(defmethod ig/halt-key! :components/event-store
  [_ _store]
  (log/info "event store stopped"))

(defn- read-event
  "Returns an event as the database gave it, with the payload and the metadata
   as data."
  [row]
  (when row
    (-> row
        (update :payload codec/decode)
        (update :metadata codec/decode))))

(defn- check-limit
  [limit]
  (when-not (and (int? limit) (<= 1 limit max-limit))
    (throw (ex-info (str ":limit must be an integer from 1 to " max-limit)
                    {:error ::invalid-limit
                     :limit limit})))
  limit)

(defn- check-position
  [after]
  (when-not (and (int? after) (not (neg? after)))
    (throw (ex-info ":after must be a position, zero or more"
                    {:error ::invalid-position
                     :after after})))
  after)

(defn get-latest-version
  "Returns the version of the last event of an aggregate, or 0 when it has none.
   - store: the value of the component
   - aggregate-id: the identity of the aggregate, a string or a UUID"
  [store aggregate-id]
  (:version (sql/execute-one!
             (:datasource store)
             [(str "SELECT COALESCE(MAX(version), 0) AS version"
                   " FROM events WHERE aggregate_id = ?")
              (str aggregate-id)])))

(defn- conflict
  "Returns the failure of an event that did not follow the one before it."
  [store
   {:keys [aggregate-id version]}]
  {:error            :version-conflict
   :aggregate-id     aggregate-id
   :version          version
   :expected-version (dec version)
   :actual-version   (get-latest-version store aggregate-id)})

(defn append!
  "Appends an event to the store, and returns it as it was stored, with its
   :position, its :id and the :created-at that the database gave it.

   The event is stored only if its :version is one more than the version of the
   last event of its aggregate, which is 0 when there is none. When it is not,
   because another command was first, nothing is stored and the result is a
   failure as data, {:error :version-conflict} with the :expected-version and
   the :actual-version: the caller reads the events again and decides. An event
   that is not valid, which is a mistake of the program, throws.
   - store: the value of the component
   - event: a map of :aggregate-id, :aggregate-type, :event-type, :payload,
     :version and, optionally, :metadata, as in borba.event-store.event"
  [store event]
  (let [valid (event/validate event)
        {:keys [aggregate-id aggregate-type event-type payload version
                metadata]} valid
        stored (try
                 (sql/execute-one!
                  (:datasource store)
                  [(str "INSERT INTO events (id, aggregate_id, aggregate_type,"
                        " event_type, payload, metadata, version)"
                        " SELECT ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?"
                        " WHERE ? - 1 = (SELECT COALESCE(MAX(version), 0)"
                        " FROM events WHERE aggregate_id = ?)"
                        " RETURNING *")
                   (random-uuid)
                   aggregate-id
                   aggregate-type
                   event-type
                   (codec/encode payload)
                   (codec/encode metadata)
                   version
                   version
                   aggregate-id])
                 (catch java.sql.SQLException cause
                   (let [data (sql/error-data cause)]
                     (if (and (= :unique-violation (:error data))
                              (= unique-constraint (:constraint data)))
                       nil
                       (throw cause)))))]
    (if stored
      (read-event stored)
      (conflict store valid))))

(defn get-events
  "Returns the events of an aggregate, in the order of their versions.
   - store: the value of the component
   - aggregate-id: the identity of the aggregate, a string or a UUID
   - from-version: the first version to return (default 1, all of them)"
  ([store aggregate-id]
   (get-events store aggregate-id {}))
  ([store
    aggregate-id
    {:keys [from-version] :or {from-version 1}}]
   (mapv read-event
         (sql/execute!
          (:datasource store)
          [(str "SELECT * FROM events WHERE aggregate_id = ? AND version >= ?"
                " ORDER BY version")
           (str aggregate-id)
           from-version]))))

(defn get-events-by-type
  "Returns the events of a type, in the order they were stored in, at most a
   limit of them. To read on, ask again with the :position of the last one as
   :after.
   - store: the value of the component
   - event-type: the name of the event, such as \"order.created\"
   - after: the position to read after (default 0, from the start)
   - limit: the most events to return, up to 10000 (default 1000)"
  ([store event-type]
   (get-events-by-type store event-type {}))
  ([store
    event-type
    {:keys [after limit] :or {after 0 limit default-limit}}]
   (mapv read-event
         (sql/execute!
          (:datasource store)
          [(str "SELECT * FROM events WHERE event_type = ? AND position > ?"
                " ORDER BY position LIMIT ?")
           event-type
           (check-position after)
           (check-limit limit)]))))

(defn get-all-events
  "Returns the events of every aggregate, in the order they were stored in, at
   most a limit of them: the feed that a projection reads. To read on, ask again
   with the :position of the last one as :after.
   - store: the value of the component
   - after: the position to read after (default 0, from the start)
   - limit: the most events to return, up to 10000 (default 1000)"
  ([store]
   (get-all-events store {}))
  ([store
    {:keys [after limit] :or {after 0 limit default-limit}}]
   (mapv read-event
         (sql/execute!
          (:datasource store)
          [(str "SELECT * FROM events WHERE position > ?"
                " ORDER BY position LIMIT ?")
           (check-position after)
           (check-limit limit)]))))

(defn get-aggregate-snapshot
  "Returns the state of an aggregate: its events, folded one after the other in
   the order of their versions, from an empty map.

   (es/get-aggregate-snapshot store order-id
     (fn [state event]
       (case (:event-type event)
         \"order.created\" (merge state (:payload event))
         \"order.shipped\" (assoc state :status :shipped)
         state)))

   - store: the value of the component
   - aggregate-id: the identity of the aggregate, a string or a UUID
   - reducer-fn: a function of the state and an event, which returns the new
     state"
  [store
   aggregate-id
   reducer-fn]
  (reduce reducer-fn {} (get-events store aggregate-id)))
