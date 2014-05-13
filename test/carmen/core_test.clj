(ns carmen.core-test
  (:require [clojure.test :refer :all]
            [carmen.core :refer :all])
  (:import [java.util.concurrent Executors]))

(deftest index-basic-operations-test
  (testing "Single threaded index operations testing."
    (clean-indexes )
    (let [key (-> (create-buffer (:size-of-key constants)) (.clear ) (.putInt (rand-int 65536)))
        chunk-meta {:position (rand-int 65536) :size (rand-int 65536)}
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
      (is (and (= @index @free-cells-registry @locked-free-cells-registry {}))))))

(defn concurrent-index-test-atom []
  (let [key (-> (create-buffer (:size-of-key constants)) (.clear ) (.putInt (rand-int 65536)))
        chunk-meta {:position (rand-int 65536) :size (rand-int 65536)}
        index-entry {(hash-buffer key) chunk-meta}]
    ;make it really breakes test
    (is (not= (put-to-index key chunk-meta) nil))
    (is (= (index-contains-key? key) true))
    (is (= (get-from-index key) chunk-meta))

    (move-from-index-to-free key)
    (is (not= (index-contains-key? key) true))
    (is (not= (get-from-index key) chunk-meta))

    (is (not= (acquire-free-cell ) nil))
    (is (= (index-contains-key? key) false))
    (is (= (get-from-index key) nil))

    (finalize-key key)
    (is (= (index-contains-key? key) false))
    (is (= (get-from-index key) nil))))

(deftest concurrent-index-basic-operations-test
  (testing "Multy threaded index operations testing."
    (clean-indexes )
    (let [max-threads 10
          pool (Executors/newFixedThreadPool max-threads)
          tasks (vec (repeat max-threads concurrent-index-test-atom))]
      (doseq [future (.invokeAll pool tasks)]
        (.get future))
      (.shutdown pool))))
