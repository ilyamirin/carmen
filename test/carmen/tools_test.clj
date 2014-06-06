(ns carmen.tools-test
  (:require [clojure.test :refer :all]
            [carmen.tools :refer :all]))

(deftest create-and-fill-buffer-test
  (testing "Create-and-fill-buffer function should produce random and filled ByteBuffers."
    (let [result-map (transient #{})]
      (doall
        (repeatedly 5000
          #(let [buffer (create-and-fill-buffer 16)]
             (conj! result-map (hash-buffer buffer)))))
      (is (= 5000 (count (persistent! result-map)))))))

(deftest hash-buffers-test
  (testing "hash-buffers should produce different results for different input."
    (let [key (create-and-fill-buffer 16)
          buffer (create-and-fill-buffer 65536)
          meta (create-and-fill-buffer 16)]
      (is
        (not=
          (hash-buffers key)
          (hash-buffers key buffer)
          (hash-buffers key buffer meta))))))

(deftest buffer-to-chunk-meta-test
  (testing "Test deserializing buffer to chunk meta map"
    (let [born (System/currentTimeMillis )]
      (-> (create-buffer (:size-of-meta constants))
        (.put 0 Byte/MAX_VALUE)
        (.putLong 1 65549)
        (.putInt 9 256)
        (.putInt 13 113)
        (.putLong 17 born)
        (.putInt 25 Integer/MAX_VALUE)
        (.putInt 29 Integer/MIN_VALUE)
        (buffer-to-meta )
        (= {:status 127
            :position 65549
            :size 256
            :cell-size 113
            :born born
            :ttl Integer/MAX_VALUE
            :checksum Integer/MIN_VALUE})
        (is)))))

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

(deftest is-expired-test
  (testing "Test expired? function, which checks expiration of chunk meta with ttl."
    (let [born (- (System/currentTimeMillis ) 5000)]
      (is (expired? {:ttl 3000 :born born}))
      (is (not (expired? {:ttl 10000 :born born})))
      (is (not (expired? {:ttl 0 :born born})))
      (is (not (expired? {:ttl -100 :born born}))))))