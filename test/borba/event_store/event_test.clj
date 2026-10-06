(ns borba.event-store.event-test
  (:require
   [borba.event-store.event :as event]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]))

(def ^:private valid
  {:aggregate-id   "order-1"
   :aggregate-type "order"
   :event-type     "order.created"
   :payload        {:total 10}
   :version        1})

(defn- problems
  "Returns the problems an event is refused for, or nil."
  [candidate]
  (try (event/validate candidate)
       nil
       (catch clojure.lang.ExceptionInfo e
         (:problems (ex-data e)))))

(deftest validate-test
  (testing "keeps a valid event, with an empty map for the metadata"
    (is (= (assoc valid :metadata {})
           (event/validate valid))))

  (testing "keeps the metadata it has"
    (is (= {:by "ana"}
           (:metadata (event/validate (assoc valid :metadata {:by "ana"}))))))

  (testing "keeps the aggregate id as text, from a string or a UUID"
    (let [id (random-uuid)]
      (is (= (str id)
             (:aggregate-id (event/validate (assoc valid :aggregate-id id)))))))

  (testing "leaves out what is not part of an event"
    (is (= (assoc valid :metadata {})
           (event/validate (assoc valid :extra "x" :position 7))))))

(deftest problems-test
  (testing "says what is wrong with each key"
    (doseq [[change expected] [[{:aggregate-id nil} ":aggregate-id"]
                               [{:aggregate-id ""} ":aggregate-id"]
                               [{:aggregate-id 5} ":aggregate-id"]
                               [{:aggregate-type " "} ":aggregate-type"]
                               [{:event-type nil} ":event-type"]
                               [{:payload "text"} ":payload"]
                               [{:payload nil} ":payload"]
                               [{:version 0} ":version"]
                               [{:version -1} ":version"]
                               [{:version 1.5} ":version"]
                               [{:version "1"} ":version"]
                               [{:metadata [1]} ":metadata"]]]
      (let [found (problems (merge valid change))]
        (is (= 1 (count found)) (pr-str change))
        (is (str/starts-with? (first found) expected) (pr-str change)))))

  (testing "says everything that is wrong at once"
    (is (= 3 (count (problems {:aggregate-id   nil
                               :aggregate-type "order"
                               :event-type     "order.created"
                               :payload        {}
                               :version        0
                               :metadata       :no})))))

  (testing "refuses a text of more than 255 characters"
    (let [text (fn [length] (apply str (repeat length "a")))]
      (is (some? (problems (assoc valid :aggregate-id (text 256)))))
      (is (nil? (problems (assoc valid :aggregate-id (text 255)))))))

  (testing "puts what is wrong in the message"
    (let [thrown (try (event/validate (assoc valid :version 0))
                      (catch clojure.lang.ExceptionInfo e e))]
      (is (str/includes? (ex-message thrown) ":version"))
      (is (= :borba.event-store.event/invalid-event
             (:error (ex-data thrown)))))))
