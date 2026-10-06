(ns borba.event-store.codec
  "The JSON of the payload and the metadata of an event, which PostgreSQL stores
   as JSONB.

   It is written as the data is, with keywords as their names, and read back
   with the keys as keywords, so a map goes in and comes out the same: a
   namespaced keyword such as :order/id is written \"order/id\" and read back as
   :order/id. What JSON has no type for does not come back as it went: a
   keyword value is a string, and a set is a vector."
  (:require
   [jsonista.core :as jsonista])
  (:import
   (org.postgresql.util PGobject)))

(set! *warn-on-reflection* true)

(defn encode
  "Returns the JSON text of a value.
   - value: the payload or the metadata of an event"
  [value]
  (jsonista/write-value-as-string value))

(defn decode
  "Returns the value of what the database gives for a JSONB column: a PGobject,
   or the text of the JSON, with its keys as keywords. A column that is NULL is
   nil.
   - column: the value of the column"
  [column]
  (cond
    (nil? column)
    nil

    (instance? PGobject column)
    (jsonista/read-value (.getValue ^PGobject column)
                         jsonista/keyword-keys-object-mapper)

    :else
    (jsonista/read-value ^String column jsonista/keyword-keys-object-mapper)))
