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

      (is (= (acquire-free-cell (+ (:size chunk-meta) 1)) nil))
      (is (and (= @index @locked-free-cells-registry {}) (= @free-cells-registry index-entry)))

      (is (= (acquire-free-cell (:size chunk-meta)) chunk-meta))
      (is (and (= @index @free-cells-registry {}) (= @locked-free-cells-registry index-entry)))

      (finalize-key key)
      (is (and (= @index @free-cells-registry @locked-free-cells-registry {}))))))

(defn concurrent-index-test-atom []
  (let [key (-> (create-buffer (:size-of-key constants)) (.clear ) (.putInt (rand-int 65536)))
        chunk-meta {:position (rand-int 65536) :size (rand-int 65536)}]
    ;make it really breakes test
    (is (not= (put-to-index key chunk-meta) nil))
    (is (= (index-contains-key? key) true))
    (is (= (get-from-index key) chunk-meta))

    (move-from-index-to-free key)
    (is (not= (index-contains-key? key) true))
    (is (not= (get-from-index key) chunk-meta))

    (is (not= (acquire-free-cell 1) nil))
    (is (= (index-contains-key? key) false))
    (is (= (get-from-index key) nil))

    (finalize-key key)
    (is (= (index-contains-key? key) false))
    (is (= (get-from-index key) nil))))

(deftest concurrent-index-basic-operations-test
  (testing "Multy threaded index operations testing."
    (clean-indexes )
    (reset-chunk-store )

    (let [max-threads 10
          pool (Executors/newFixedThreadPool max-threads)
          tasks (vec (repeat max-threads concurrent-index-test-atom))]
      (doseq [future (.invokeAll pool tasks)]
        (.get future))
      (.shutdown pool))))

;;TODO: add concurrent b-ops test
;;TODO: add storage size atom control

(deftest chunk-business-operations-test
  (testing "Test chunks persist/read/remove operations."
    (clean-indexes )

    (reset-chunk-store )
    (is (= (.size get-chunk-store) 0))

    (let [key (-> (create-buffer 16) (.clear ) (.putInt 0 (rand-int 65536)))
          chunk-body (-> (create-buffer 256) (.clear ) (.putInt 0 (rand-int 65536)))
          key1 (-> (create-buffer 16) (.clear ) (.putInt 0 (rand-int 65536)))
          chunk-body1 (-> (create-buffer 256) (.clear ) (.putInt 0 (rand-int 65536)))]

      (persist-chunk key chunk-body)

      (let [get-chunk-result (get-chunk key)]
        (is (= (hash-buffer get-chunk-result) (hash-buffer chunk-body)))
        (is (= (.getInt (.rewind get-chunk-result) 0) (.getInt (.rewind chunk-body) 0))))
      (is (= (get-chunk key1) nil))
      (is (= (.size get-chunk-store) (+ (:size-of-meta constants) (:size-of-key constants) (.capacity chunk-body))))

      (remove-chunk key)
      (is (= (get-chunk key) nil))
      (is (= (.size get-chunk-store) (+ (:size-of-meta constants) (:size-of-key constants) (.capacity chunk-body))))

      (persist-chunk key1 chunk-body1)
      (let [get-chunk-result (get-chunk key1)]
        (is (= (hash-buffer get-chunk-result) (hash-buffer chunk-body1)))
        (is (= (.getInt (.rewind get-chunk-result) 0) (.getInt (.rewind chunk-body1) 0))))
      (is (= (get-chunk key) nil))
      (is (= (.size get-chunk-store) (+ (:size-of-meta constants) (:size-of-key constants) (.capacity chunk-body)))))))
