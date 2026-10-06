(ns borba.event-store-test
  (:require
   [borba.event-store :as store]
   [clojure.test :refer [deftest is testing]]))

(defn- thrown-error
  [f]
  (try (f)
       nil
       (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))

(def ^:private no-database {:datasource nil})

(deftest limits-test
  (testing "refuses a limit that is not from 1 to 10000, before a query"
    (doseq [bad [0 -1 10001 1.5 "10" nil]]
      (is (= :borba.event-store/invalid-limit
             (thrown-error
              #(store/get-events-by-type no-database "t" {:limit bad})))
          (pr-str bad))
      (is (= :borba.event-store/invalid-limit
             (thrown-error
              #(store/get-all-events no-database {:limit bad})))
          (pr-str bad))))

  (testing "refuses a position that is not zero or more, before a query"
    (doseq [bad [-1 1.5 "0" nil]]
      (is (= :borba.event-store/invalid-position
             (thrown-error
              #(store/get-events-by-type no-database "t" {:after bad})))
          (pr-str bad))
      (is (= :borba.event-store/invalid-position
             (thrown-error
              #(store/get-all-events no-database {:after bad})))
          (pr-str bad)))))

(deftest append-validation-test
  (testing "refuses an event that is not valid, before a query"
    (is (= :borba.event-store.event/invalid-event
           (thrown-error #(store/append! no-database {:version 0}))))))
