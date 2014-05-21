(ns carmen.core-test
  (:require [clojure.test :refer :all]
            [carmen.tools :refer :all]
            [carmen.index :refer :all]
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

      (let [free-cell (acquire-free-cell (:size chunk-meta))]
        (is (= (first free-cell) (hash-buffer key)))
        (is (= (second free-cell) chunk-meta))
        (is (and (= @index @free-cells-registry {}) (= @locked-free-cells-registry index-entry)))

        (finalize-free-cell (hash-buffer key))
        (is (and (= @index @free-cells-registry @locked-free-cells-registry {}))))

      (put-to-free key chunk-meta)
      (is (and (= @index @locked-free-cells-registry {}) (= @free-cells-registry index-entry))))))

;TODO make it really breakes the test
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

;TODO: test b functions results
(deftest chunk-business-operations-test
  (testing "Test chunks persist/read/remove operations."
    (clean-indexes )
    (is (= @index @free-cells-registry @locked-free-cells-registry {}))
    
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

      (is (= (.size get-chunk-store) (+ (:size-of-meta constants) (:size-of-key constants) (.capacity chunk-body))))
          
      (clean-indexes)
      (is (= @index @free-cells-registry @locked-free-cells-registry {}))
      (is (load-existed-chunk-key (load-existed-chunk-meta 0)) key1)

      (clean-indexes)      
      (is (load-whole-existed-storage ))
        
	    (is (= (get-chunk key) nil))
	    (let [get-chunk-result (get-chunk key1)]
       (is (= (hash-buffer get-chunk-result) (hash-buffer chunk-body1)))
       (is (= (.getInt (.rewind get-chunk-result) 0) (.getInt (.rewind chunk-body1) 0)))))))

;;TODO: add concurrent b-ops test
(deftest chunk-business-operations-test
  (testing "Concurrent test of chunks persist/read/remove operations."
    (clean-indexes )
    (reset-chunk-store )

    (def keys-to-persist
      (ref
        (vec
          (repeat 10
            (-> (create-buffer 16) (.clear ) (.putInt 0 (rand-int Integer/MAX_VALUE)))))))

    (def chunks (ref {}))
    (def removed-chunks (ref {}))

    ;;TODO; add exit latch
    ;;TODO: add storage size control
    ;;TODO: add final control
    (defn one-operation-quad []
      (let [key (-> (create-buffer 16) (.clear ) (.putInt 0 (rand-int Integer/MAX_VALUE)));(peek @keys-to-persist)
            chunk-body (-> (create-buffer (rand-int 65536)) (.clear ) (.putInt 0 (rand-int Integer/MAX_VALUE)))] ;fill and check the whole chunk

        (time (persist-chunk key chunk-body))
;        (dosync (alter keys-to-persist pop))

        (dosync (alter chunks assoc (hash-buffer key) chunk-body))

        (is (= (hash-buffer (get-chunk key)) (hash-buffer chunk-body)))
        (is (= (.getInt (get-chunk key) 0) (.getInt chunk-body 0)))

        (if (> (rand) 0.5)
          (do
            (time (remove-chunk key))
            (is (nil? (get-chunk key)))))

        ;(dosync
         ; (alter removed-chunks assoc (hash-buffer key) chunk-body)
          ;(alter chunks dissoc (hash-buffer key))
         ; )
        ))

    (defn repeated-quad [n]
      (dorun n
        (repeatedly n #(one-operation-quad ))))

    (map deref [(future (repeated-quad 100)) (future (repeated-quad 100))])
    (Thread/sleep 2000)
    (println "count= " (count @chunks))
    (println (keys @chunks))
    (println (keys @removed-chunks))
    ))

(deftest buffer-to-chunk-meta-test
  (testing "Test deserializing buffer to chunk meta map"
    (-> (create-buffer (:size-of-meta constants))
      (.put 0 Byte/MAX_VALUE)
      (.putLong 1 65549)
      (.putInt 9 256)
      (buffer-to-meta)
      (is {:status 127, :position 65549, :size 256}))))
