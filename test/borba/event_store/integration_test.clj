(ns ^:integration borba.event-store.integration-test
  "Runs against a real PostgreSQL, which the pipeline provides and which a
   developer starts with

     docker run --rm -d --name borba-sql-it -p 127.0.0.1:55432:5432 \\
       -e POSTGRES_USER=ci -e POSTGRES_PASSWORD=ci -e POSTGRES_DB=ci \\
       postgres:18.6-alpine

   and points the tests at with DATABASE_URL, DATABASE_USER and
   DATABASE_PASSWORD (jdbc:postgresql://127.0.0.1:55432/ci, ci and ci)."
  (:require
   [borba.event-store :as store]
   [borba.sql-client :as sql]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [integrant.core :as ig])
  (:import
   (java.time Instant)
   (java.util.concurrent CountDownLatch TimeUnit)))

(set! *warn-on-reflection* true)

(def ^:private latch-wait-seconds 10)
(def ^:private append-only-state "23001")

(defn- environment
  [variable]
  (or (System/getenv variable)
      (throw (ex-info (str "set " variable " to run the integration tests")
                      {:variable variable}))))

(def ^:dynamic *system*
  "The system of the tests, bound for the run of the namespace."
  nil)

(defn- with-event-store
  "Starts the database and the event store, and drops the table after."
  [run-tests]
  (let [system (ig/init
                {:components/database
                 {:jdbc-url (environment "DATABASE_URL")
                  :username (environment "DATABASE_USER")
                  :password (environment "DATABASE_PASSWORD")}
                 :components/event-store
                 {:database (ig/ref :components/database)}})]
    (try
      (binding [*system* system]
        (run-tests))
      (finally
        (sql/execute! (:components/database system)
                      ["DROP TABLE IF EXISTS events"])
        (sql/execute! (:components/database system)
                      ["DROP FUNCTION IF EXISTS events_are_append_only()"])
        (ig/halt! system)))))

(use-fixtures :once with-event-store)

(defn- es [] (:components/event-store *system*))
(defn- ds [] (:components/database *system*))

(defn- aggregate-id [] (str "agg-" (random-uuid)))

(defn- event
  [id version more]
  (merge {:aggregate-id   id
          :aggregate-type "order"
          :event-type     "order.created"
          :payload        {:total 10}
          :version        version}
         more))

(deftest append-test
  (testing "stores an event and returns it, as stored"
    (let [id     (aggregate-id)
          stored (store/append! (es) (event id 1 {:payload {:total 10}}))]
      (is (= {:aggregate-id   id
              :aggregate-type "order"
              :event-type     "order.created"
              :payload        {:total 10}
              :metadata       {}
              :version        1}
             (select-keys stored [:aggregate-id :aggregate-type :event-type
                                  :payload :metadata :version])))
      (is (pos? (:position stored)))
      (is (uuid? (:id stored)))
      (is (instance? Instant (:created-at stored)))))

  (testing "keeps the metadata"
    (let [id     (aggregate-id)
          stored (store/append! (es) (event id 1 {:metadata {:by "ana"}}))]
      (is (= {:by "ana"} (:metadata stored)))))

  (testing "takes a UUID as the identity of the aggregate"
    (let [id (random-uuid)]
      (store/append! (es) (event id 1 {}))
      (is (= [1] (mapv :version (store/get-events (es) id))))
      (is (= [1] (mapv :version (store/get-events (es) (str id)))))))

  (testing "gives each event the next position"
    (let [first-event  (store/append! (es) (event (aggregate-id) 1 {}))
          second-event (store/append! (es) (event (aggregate-id) 1 {}))]
      (is (< (:position first-event) (:position second-event))))))

(deftest payload-test
  (testing "reads the payload back as it was written, with keys as keywords"
    (let [id      (aggregate-id)
          payload {:order/id "o-1"
                   :items    [{:sku "a" :qty 2} {:sku "b" :qty 1}]
                   :note     "Ação, não reação"
                   :paid?    false
                   :coupon   nil}
          stored  (store/append! (es) (event id 1 {:payload payload}))]
      (is (= payload (:payload stored)))
      (is (= payload (:payload (first (store/get-events (es) id)))))))

  (testing "a value JSON has no type for comes back as the closest it has"
    (let [id (aggregate-id)]
      (store/append! (es) (event id 1 {:payload {:status :shipped}}))
      (is (= {:status "shipped"}
             (:payload (first (store/get-events (es) id))))))))

(deftest version-test
  (testing "the first event of an aggregate is version 1"
    (is (= 0 (store/get-latest-version (es) (aggregate-id)))))

  (testing "each event is one more than the one before"
    (let [id (aggregate-id)]
      (store/append! (es) (event id 1 {}))
      (store/append! (es) (event id 2 {}))
      (is (= 2 (store/get-latest-version (es) id)))))

  (testing "an event that skips a version is a conflict, as data, and is
            not stored"
    (let [id (aggregate-id)]
      (store/append! (es) (event id 1 {}))
      (is (= {:error            :version-conflict
              :aggregate-id     id
              :version          3
              :expected-version 2
              :actual-version   1}
             (store/append! (es) (event id 3 {}))))
      (is (= [1] (mapv :version (store/get-events (es) id))))))

  (testing "an event at a version that is taken is a conflict"
    (let [id (aggregate-id)]
      (store/append! (es) (event id 1 {}))
      (is (= {:error :version-conflict :expected-version 0 :actual-version 1}
             (select-keys (store/append! (es) (event id 1 {}))
                          [:error :expected-version :actual-version])))))

  (testing "the first event of an aggregate cannot be version 2"
    (is (= :version-conflict
           (:error (store/append! (es) (event (aggregate-id) 2 {})))))))

(deftest concurrency-test
  (testing "of several commands that append the same version, one wins"
    (let [id      (aggregate-id)
          threads 8
          ready   (CountDownLatch. threads)
          go      (CountDownLatch. 1)
          results (mapv (fn [n]
                          (future
                            (.countDown ready)
                            (.await go latch-wait-seconds TimeUnit/SECONDS)
                            (store/append! (es)
                                           (event id 1 {:payload {:n n}}))))
                        (range threads))]
      (.await ready latch-wait-seconds TimeUnit/SECONDS)
      (.countDown go)
      (let [outcomes (mapv deref results)]
        (is (= 1 (count (remove :error outcomes))))
        (is (= (dec threads)
               (count (filter #(= :version-conflict (:error %)) outcomes))))
        (is (= 1 (count (store/get-events (es) id))))))))

(deftest reading-test
  (let [id (aggregate-id)]
    (doseq [version (range 1 6)]
      (store/append! (es) (event id version {:payload {:v version}})))

    (testing "reads the events of an aggregate in the order of their versions"
      (is (= [1 2 3 4 5] (mapv :version (store/get-events (es) id)))))

    (testing "reads from a version"
      (is (= [4 5] (mapv :version
                         (store/get-events (es) id {:from-version 4})))))

    (testing "has no events for an aggregate that has none"
      (is (= [] (store/get-events (es) (aggregate-id)))))

    (testing "folds the events into the state of the aggregate"
      (is (= {:sum 15}
             (store/get-aggregate-snapshot
              (es)
              id
              (fn [state stored]
                (update state :sum (fnil + 0)
                        (get-in stored [:payload :v])))))))))

(deftest paging-test
  (let [kind   (str "paging." (random-uuid))
        ids    (repeatedly 5 aggregate-id)
        stored (mapv #(store/append! (es) (event % 1 {:event-type kind}))
                     ids)]

    (testing "reads the events of a type in the order they were stored in"
      (is (= (mapv :id stored)
             (mapv :id (store/get-events-by-type (es) kind)))))

    (testing "reads at most a limit, and on from the position of the last"
      (let [first-page  (store/get-events-by-type (es) kind {:limit 2})
            second-page (store/get-events-by-type
                         (es)
                         kind
                         {:limit 2 :after (:position (last first-page))})
            third-page  (store/get-events-by-type
                         (es)
                         kind
                         {:limit 2 :after (:position (last second-page))})]
        (is (= 2 (count first-page)))
        (is (= 2 (count second-page)))
        (is (= 1 (count third-page)))
        (is (= (mapv :id stored)
               (mapv :id (concat first-page second-page third-page))))))

    (testing "reads the events of every aggregate, in the order they were
            stored in"
      (let [after (dec (:position (first stored)))
            all   (store/get-all-events (es) {:after after :limit 5})]
        (is (= (mapv :position stored) (mapv :position all)))))

    (testing "has nothing to read past the last"
      (is (= [] (store/get-events-by-type
                 (es)
                 kind
                 {:after (:position (last stored))}))))))

(deftest append-only-test
  (let [id (aggregate-id)
        _  (store/append! (es) (event id 1 {}))
        sql-state (fn [statement]
                    (try (sql/execute! (ds) statement)
                         nil
                         (catch java.sql.SQLException e (.getSQLState e))))]
    (testing "refuses an UPDATE"
      (is (= append-only-state
             (sql-state ["UPDATE events SET event_type = 'x'
                          WHERE aggregate_id = ?" id]))))

    (testing "refuses a DELETE"
      (is (= append-only-state
             (sql-state ["DELETE FROM events WHERE aggregate_id = ?" id]))))

    (testing "refuses a TRUNCATE"
      (is (= append-only-state (sql-state ["TRUNCATE events"]))))

    (testing "leaves the event as it was"
      (is (= [1] (mapv :version (store/get-events (es) id)))))))

(deftest schema-test
  (testing "starting again, or together, finds the schema made"
    (let [database (ds)
          starts   (mapv (fn [_]
                           (future
                             (ig/init-key :components/event-store
                                          {:database database})))
                         (range 4))]
      (is (every? #(= {:datasource database} %) (mapv deref starts)))
      (is (some? (store/append! (es) (event (aggregate-id) 1 {}))))))

  (testing "needs the database"
    (is (= :borba.event-store/no-database
           (try (ig/init-key :components/event-store {})
                (catch clojure.lang.ExceptionInfo e (:error (ex-data e))))))))
