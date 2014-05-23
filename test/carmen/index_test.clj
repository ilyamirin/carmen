(ns carmen.index-test
  (:require [clojure.test :refer :all]
            [carmen.tools :refer :all]
            [carmen.index :refer :all]))

(deftest index-basic-operations-test
  (testing "Single-threaded index operations test."
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

      (is (= (acquire-free-cell (+ (:size chunk-meta) 1)) nil))
      (is (and (= @index @locked-free-cells-registry {}) (= @free-cells-registry index-entry)))

      (let [free-cell (acquire-free-cell (:size chunk-meta))]
        (is (= (first free-cell) (hash-buffer key)))
        (is (= (second free-cell) chunk-meta))
        (is (and (= @index @free-cells-registry {}) (= @locked-free-cells-registry index-entry)))

        (finalize-free-cell (hash-buffer key))
        (is (and (= @index @free-cells-registry @locked-free-cells-registry {}))))

      (put-to-free key chunk-meta)
      (is (and (= @index @locked-free-cells-registry {}) (= @free-cells-registry index-entry))))))

;TODO make it really breakes the test

(deftest concurrent-index-basic-operations-test
  (testing "Multy-threaded index operations test."
    (defn concurrent-index-test-atom []
      (let [key (-> (create-buffer (:size-of-key constants)) (.clear ) (.putInt (rand-int 65536)))
            chunk-meta {:position (rand-int 65536) :size (rand-int 65536)}]
        (is (not= (put-to-index key chunk-meta) nil))
        (is (= (index-contains-key? key) true))
        (is (= (get-from-index key) chunk-meta))

        (move-from-index-to-free key)
        (is (not= (index-contains-key? key) true))
        (is (not= (get-from-index key) chunk-meta))

        (is (not= (acquire-free-cell 1) nil))
        (is (= (index-contains-key? key) false))
        (is (= (get-from-index key) nil))

        (finalize-free-cell (hash-buffer key))
        (is (= (index-contains-key? key) false))
        (is (= (get-from-index key) nil)))
      (println "One thread of index test has just finished."))

    (clean-indexes )

    (dorun (apply pcalls (repeat 5 concurrent-index-test-atom)))))
