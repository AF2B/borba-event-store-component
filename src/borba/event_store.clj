(ns borba.event-store
  "Integrant component for an append-only Event Store backed by PostgreSQL.

   Registers :components/event-store. Requires :components/database to be
   started first.

   Provides a full Event Sourcing API:
     append!            — persist a new event
     get-events         — replay all events for an aggregate
     get-events-by-type — query by event type
     get-latest-version — current version counter for an aggregate

   Usage:
     (let [{:keys [event-store]} components]
       (es/append! event-store
         {:aggregate-id   order-id
          :aggregate-type \"order\"
          :event-type     \"order.created\"
          :payload        order-data
          :version        1}))"
  (:require [integrant.core :as ig]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [cheshire.core :as json])
  (:import (java.util UUID)
           (java.time Instant)))

;; ── Schema ───────────────────────────────────────────────────────────────────

(def ^:private create-table-sql
  "CREATE TABLE IF NOT EXISTS events (
     id             UUID PRIMARY KEY,
     aggregate_id   VARCHAR(255) NOT NULL,
     aggregate_type VARCHAR(255) NOT NULL,
     event_type     VARCHAR(255) NOT NULL,
     payload        JSONB NOT NULL,
     metadata       JSONB DEFAULT '{}',
     version        BIGINT NOT NULL,
     created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
     UNIQUE (aggregate_id, version)
   );
   CREATE INDEX IF NOT EXISTS idx_events_aggregate ON events (aggregate_id, version);
   CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);")

;; ── Integrant lifecycle ──────────────────────────────────────────────────────

(defmethod ig/init-key :components/event-store
  [_ {:keys [database]}]
  (jdbc/execute! database [create-table-sql])
  (println "📜 [event-store] Initialized")
  {:datasource database})

(defmethod ig/halt-key! :components/event-store
  [_ _]
  (println "📜 [event-store] Stopped"))

;; ── Public API ───────────────────────────────────────────────────────────────

(defn append!
  "Appends an event to the store. Returns the persisted event map.

   Required keys in event map:
     :aggregate-id   — identity of the aggregate (e.g. order UUID)
     :aggregate-type — domain type string (e.g. \"order\")
     :event-type     — event name (e.g. \"order.created\")
     :payload        — arbitrary map (stored as JSONB)
     :version        — monotonically increasing integer

   Optional:
     :metadata — additional map (stored as JSONB)"
  [{:keys [datasource]} {:keys [aggregate-id aggregate-type event-type payload metadata version]}]
  (jdbc/execute-one!
   datasource
   ["INSERT INTO events
       (id, aggregate_id, aggregate_type, event_type, payload, metadata, version, created_at)
       VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?)"
    (UUID/randomUUID)
    aggregate-id
    aggregate-type
    event-type
    (json/generate-string payload)
    (json/generate-string (or metadata {}))
    version
    (Instant/now)]
   {:builder-fn rs/as-unqualified-kebab-maps}))

(defn get-events
  "Returns all events for aggregate-id in ascending version order."
  [{:keys [datasource]} aggregate-id]
  (jdbc/execute!
   datasource
   ["SELECT * FROM events WHERE aggregate_id = ? ORDER BY version ASC" aggregate-id]
   {:builder-fn rs/as-unqualified-kebab-maps}))

(defn get-events-by-type
  "Returns all events with the given event-type, oldest first."
  [{:keys [datasource]} event-type]
  (jdbc/execute!
   datasource
   ["SELECT * FROM events WHERE event_type = ? ORDER BY created_at ASC" event-type]
   {:builder-fn rs/as-unqualified-kebab-maps}))

(defn get-latest-version
  "Returns the latest version number for aggregate-id, or 0 if none exists."
  [{:keys [datasource]} aggregate-id]
  (let [result (jdbc/execute-one!
                datasource
                ["SELECT COALESCE(MAX(version), 0) AS version FROM events WHERE aggregate_id = ?"
                 aggregate-id]
                {:builder-fn rs/as-unqualified-kebab-maps})]
    (:version result 0)))

(defn get-aggregate-snapshot
  "Replays all events for aggregate-id and reduces them with reducer-fn.
   reducer-fn receives [accumulator event-map] and returns new accumulator.

   Example:
     (es/get-aggregate-snapshot store order-id
       (fn [state event]
         (case (:event-type event)
           \"order.created\" (merge state (:payload event))
           \"order.shipped\" (assoc state :status :shipped)
           state)))"
  [{:keys [datasource]} aggregate-id reducer-fn]
  (let [events (jdbc/execute!
                datasource
                ["SELECT * FROM events WHERE aggregate_id = ? ORDER BY version ASC" aggregate-id]
                {:builder-fn rs/as-unqualified-kebab-maps})]
    (reduce reducer-fn {} events)))
