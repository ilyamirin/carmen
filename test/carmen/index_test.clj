(ns carmen.index-test
  (:require [clojure.test :refer :all]
            [carmen.tools :refer :all]
            [carmen.index :refer :all]))

(deftest concurrent-index-basic-operations-test
  (testing "Index operations test."
    (def memory (create-memory ))

    (defn concurrent-index-test-atom []
      (let [key (create-and-fill-buffer 16)
            chunk-meta {:position (rand-int 65536) :size (rand-int 65536)}]
        (is (not= (put-to-index memory key chunk-meta) nil))
        (is (= (index-contains-key? memory key) true))
        (is (= (get-from-index memory key) chunk-meta))

        (move-from-index-to-free memory key)
        (is (not= (index-contains-key? memory key) true))
        (is (not= (get-from-index memory key) chunk-meta))

        (is (not= (acquire-free-cell memory 1) nil))
        (is (= (index-contains-key? memory key) false))
        (is (= (get-from-index memory key) nil))

        (finalize-free-cell memory (hash-buffer key))
        (is (= (index-contains-key? memory key) false))
        (is (= (get-from-index memory key) nil))))

    (clean-index memory)

    ;;;100000 is too much
    (dorun (apply pcalls (repeat 10000 concurrent-index-test-atom)))))