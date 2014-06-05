(ns carmen.index-test
  (:require [clojure.test :refer :all]
            [carmen.tools :refer :all]
            [carmen.index :refer :all]))

(deftest index-basic-operations-test
  (testing "Index operations test."
    (def memory (create-memory ))

    (clean-indexes memory)

    (let [key (create-and-fill-buffer 16)
          chunk-meta {:position (rand-int 65536) :size 256}]
      (is (not= (put-to-index memory key chunk-meta) nil))
      (is (= (index-contains-key? memory key) true))
      (is (= (get-from-index memory key) chunk-meta))

      (move-from-index-to-free memory key)
      (is (not= (index-contains-key? memory key) true))
      (is (not= (get-from-index memory key) chunk-meta))

      (is (nil? (acquire-free memory 1)))
      (is (nil? (acquire-free memory 257)))
      (is (not= (acquire-free memory (:size chunk-meta)) nil))
      (is (= (index-contains-key? memory key) false))
      (is (= (get-from-index memory key) nil)))))

(deftest concurrent-index-basic-operations-test
  (testing "Concurrent index operations test."
    (def memory (create-memory ))

    (clean-indexes memory)

    (defn concurrent-index-test-atom []
      (let [key (create-and-fill-buffer 16)
            chunk-meta {:position (rand-int 65536) :size 65536}]
        (is (not= (put-to-index memory key chunk-meta) nil))
        (is (= (index-contains-key? memory key) true))
        (is (= (get-from-index memory key) chunk-meta))

        (move-from-index-to-free memory key)
        (is (not= (index-contains-key? memory key) true))
        (is (not= (get-from-index memory key) chunk-meta))

        (is (not= (acquire-free memory (:size chunk-meta)) nil))
        (is (= (index-contains-key? memory key) false))
        (is (= (get-from-index memory key) nil))))

    (dorun (apply pcalls (repeat 10000 concurrent-index-test-atom)))))