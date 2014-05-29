(ns carmen.tools-test
  (:require [clojure.test :refer :all]
            [carmen.tools :refer :all]))

(deftest buffer-to-chunk-meta-test
  (testing "Test deserializing buffer to chunk meta map"
    (-> (create-buffer (:size-of-meta constants))
      (.put 0 Byte/MAX_VALUE)
      (.putLong 1 65549)
      (.putInt 9 256)
      (.putInt 13 113)
      (buffer-to-meta)
      (= {:status 127, :position 65549, :size 256, :cell-size 113})
      (is))))

(deftest apply-consistently-test
  (testing "Test apply-consistently macros whitch is used for replicaopertions for Storages"
    (is (= false (apply-consistently :quorum = [2 3 3 4 5] 1)))
    (is (= false (apply-consistently :quorum = [2 3 1 4 5] 1)))
    (is (= false (apply-consistently :quorum = [1 3 1 4 5] 1)))
    (is (= true (apply-consistently :quorum = [2 1 1 4 1] 1)))

    (is (= false (apply-consistently :one = [2 3 3 4 5] 1)))
    (is (= true (apply-consistently :one = [2 3 1 4 5] 1)))
    (is (= true (apply-consistently :one = [2 3 3 4 1] 1)))

    (is (= false (apply-consistently :all = [1 3 1 4 5] 1)))
    (is (= true (apply-consistently :all = [1 1 1] 1)))))