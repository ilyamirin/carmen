(ns carmen.core-test
  (:require [clojure.test :refer :all]
            [carmen.core :refer :all])
  (:import [java.util.concurrent Executors]))

(defn atomic-index-test []
  (let [key (-> (create-buffer (:size-of-key constants)) (.clear ) (.putInt 6565))
        chunk-meta {:position 21 :size 65536}
        index-entry {(hash-buffer key) chunk-meta}]
    (is (= (put-to-index key chunk-meta) index-entry))
    (is (and (= @index index-entry) (= @locked-free-cells-registry @free-cells-registry {} )))

    (is (= (index-contains-key? key) true))
    (is (and (= @index index-entry) (= @locked-free-cells-registry @free-cells-registry {} )))

    (is (= (get-from-index key) chunk-meta))
    (is (and (= @index index-entry) (= @locked-free-cells-registry @free-cells-registry {} )))

    (move-from-index-to-free key)
    (is (and (= @index @locked-free-cells-registry {}) (= @free-cells-registry index-entry)))

    (acquire-free-cell )
    (is (and (= @index @free-cells-registry {}) (= @locked-free-cells-registry index-entry)))

    (finalize-key key)
    (is (and (= @index @free-cells-registry @locked-free-cells-registry {})))))

(deftest index-basic-operations-test
  (testing "Single threaded index operations testing."
    (atomic-index-test )))

(deftest concurrent-index-basic-operations-test
  (testing "Single threaded index operations testing."
    (let [pool (Executors/newFixedThreadPool 2)]
      (doseq [future (.invokeAll pool [atomic-index-test atomic-index-test])]
        (.get future))
      ;;(.shutdown pool)
      ;;(map deref refs)
      )))
