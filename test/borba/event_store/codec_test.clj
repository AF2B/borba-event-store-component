(ns borba.event-store.codec-test
  (:require
   [borba.event-store.codec :as codec]
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check.clojure-test :as tct]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop])
  (:import
   (org.postgresql.util PGobject)))

(set! *warn-on-reflection* true)

(defn- jsonb
  "A value as the driver gives a JSONB column."
  [^String text]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue text)))

(deftest encode-test
  (testing "writes data as JSON, with keywords as their names"
    (is (= "{\"a\":1,\"b\":[true,null,\"x\"]}"
           (codec/encode {:a 1 :b [true nil "x"]})))
    (is (= "{\"order/id\":\"o-1\"}" (codec/encode {:order/id "o-1"}))))

  (testing "writes what JSON has no type for as the closest it has"
    (is (= "{\"state\":\"shipped\"}" (codec/encode {:state :shipped})))
    (is (= "[1]" (codec/encode #{1})))))

(deftest decode-test
  (testing "reads a JSONB column, with keys as keywords"
    (is (= {:a 1 :b {:c [1 2]}}
           (codec/decode (jsonb "{\"a\":1,\"b\":{\"c\":[1,2]}}")))))

  (testing "reads the text of JSON too"
    (is (= {:a 1} (codec/decode "{\"a\":1}"))))

  (testing "reads a namespaced key back as it was"
    (is (= {:order/id "o-1"} (codec/decode (jsonb "{\"order/id\":\"o-1\"}")))))

  (testing "is nil for a column that is NULL"
    (is (nil? (codec/decode nil))))

  (testing "reads what is not ASCII"
    (is (= {:name "Ação"} (codec/decode (jsonb "{\"name\":\"Ação\"}"))))))

(def ^:private json-value
  (gen/recursive-gen
   (fn [inner]
     (gen/one-of [(gen/vector inner)
                  (gen/map gen/keyword inner)]))
   (gen/one-of [gen/small-integer
                gen/string-alphanumeric
                gen/boolean
                (gen/return nil)])))

(tct/defspec what-goes-in-comes-out
  (prop/for-all [value json-value]
    (= value (codec/decode (codec/encode value)))))
