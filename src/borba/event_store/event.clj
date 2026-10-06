(ns borba.event-store.event
  "The event that is appended to the store, checked.

   An event has:

     :aggregate-id    the identity of the aggregate it belongs to: a string,
                      or a UUID, which is kept as its text
     :aggregate-type  the kind of aggregate, a non-empty string such as
                      \"order\"
     :event-type      the name of the event, a non-empty string such as
                      \"order.created\"
     :payload         what happened, a map
     :version         its place in the history of the aggregate: 1 for the
                      first, and always one more than the event before it
     :metadata        who and why, a map (optional)

   Everything wrong with an event is found at once, and said, so an event that
   cannot be stored is not found out one key at a time."
  (:require
   [clojure.string :as str]))

(def ^:private max-text-length 255)

(defn- text?
  [value]
  (and (string? value)
       (not (str/blank? value))
       (<= (count value) max-text-length)))

(defn- problems
  "Returns what is wrong with an event, as a vector of messages."
  [{:keys [aggregate-id aggregate-type event-type payload version metadata]}]
  (cond-> []
    (not (or (text? aggregate-id) (uuid? aggregate-id)))
    (conj ":aggregate-id must be a string of up to 255 characters, or a UUID")

    (not (text? aggregate-type))
    (conj ":aggregate-type must be a non-empty string of up to 255 characters")

    (not (text? event-type))
    (conj ":event-type must be a non-empty string of up to 255 characters")

    (not (map? payload))
    (conj ":payload must be a map")

    (not (and (int? version) (pos? version)))
    (conj ":version must be a positive integer, 1 for the first event")

    (not (or (nil? metadata) (map? metadata)))
    (conj ":metadata must be a map, or not given")))

(defn validate
  "Returns the event, with the aggregate id as text and the metadata as a map,
   or throws naming everything that is wrong with it.
   - event: a map of :aggregate-id, :aggregate-type, :event-type, :payload,
     :version and, optionally, :metadata"
  [event]
  (if-let [found (seq (problems event))]
    (throw (ex-info (str "the event is not valid: " (str/join "; " found))
                    {:error    ::invalid-event
                     :problems (vec found)}))
    (-> (select-keys event [:aggregate-id :aggregate-type :event-type :payload
                            :version :metadata])
        (update :aggregate-id str)
        (update :metadata #(or % {})))))
