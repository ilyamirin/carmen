(ns carmen.core-test
  (:require [clojure.test :refer :all]
            [carmen.tools :refer :all]
            [carmen.index :refer :all]
            [carmen.core :refer :all]))

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

      (clean-indexes)      
      (is (load-whole-existed-storage ))
        
	    (is (= (get-chunk key) nil))
	    (let [get-chunk-result (get-chunk key1)]
       (is (= (hash-buffer get-chunk-result) (hash-buffer chunk-body1)))
       (is (= (.getInt (.rewind get-chunk-result) 0) (.getInt (.rewind chunk-body1) 0)))))))

(deftest chunk-business-operations-test
  (testing "Concurrent test of chunks persist/read/remove operations."
    (clean-indexes )
    (reset-chunk-store )

    (def chunks (ref {}))
    (def removed-chunks (ref {}))

    (defn one-operation-quad []
      (let [key (create-and-fill-buffer 16)
            chunk-body (create-and-fill-buffer (+ 4 (rand-int 65536)))]

        (persist-chunk key chunk-body)

        (is (= (hash-buffer (get-chunk key)) (hash-buffer chunk-body)))
        (is (= (.getInt (get-chunk key) 0) (.getInt chunk-body 0)))

        (if (> (rand) 0.5)
          (do
            (remove-chunk key)
            (is (nil? (get-chunk key)))
            (dosync (alter removed-chunks assoc key chunk-body)))
          (dosync (alter chunks assoc key chunk-body)))))

    (defn repeated-quad [n]
      (dorun n
        (repeatedly n #(one-operation-quad)))
      (println "One testing thread has finished at" (System/currentTimeMillis)))

    (defn chunks-is-equal? [chunk1 chunk2]
      (and
        (= chunk1 chunk2)
        (= (hash-buffer chunk1) (hash-buffer chunk2))))

    (let [start (System/currentTimeMillis)]
      (dorun (pvalues (repeated-quad 1000) (repeated-quad 1000) (repeated-quad 1000)))
      (println (count @chunks) "chunks processed for" (- (System/currentTimeMillis) start) "mseconds"))

    (doall (map #(is (chunks-is-equal? (get-chunk %) (get @chunks %))) (keys @chunks)))
    (doall (map #(is (not (get-chunk %))) (keys @removed-chunks)))

    (let [key-meta-space (+ (:size-of-meta constants) (:size-of-key constants))
          summary-space (reduce #(+ %1 (.capacity %2) key-meta-space) 0 (vals @chunks))]
      (println (int (/ summary-space 1000000)) "Mb of space was used")
      (is (< (.size get-chunk-store) (* summary-space 1.5))))

    (clean-indexes )
    (is (load-whole-existed-storage))

    (doall (map #(is (chunks-is-equal? (get-chunk %) (get @chunks %))) (keys @chunks)))
    (doall (map #(is (not (get-chunk %))) (keys @removed-chunks)))

    (let [start (System/currentTimeMillis)
          old-count (count @chunks)]
      (dorun (pvalues (repeated-quad 1000) (repeated-quad 1000) (repeated-quad 1000)))
      (println (- (count @chunks) old-count) "chunks processed for" (- (System/currentTimeMillis) start) "mseconds"))

    (doall (map #(is (chunks-is-equal? (get-chunk %) (get @chunks %))) (keys @chunks)))
    (doall (map #(is (not (get-chunk %))) (keys @removed-chunks)))

    (let [key-meta-space (+ (:size-of-meta constants) (:size-of-key constants))
          summary-space (reduce #(+ %1 (.capacity %2) key-meta-space) 0 (vals @chunks))]
      (println (int (/ summary-space 1000000)) "Mb of space was used")
      (is (< (.size get-chunk-store) (* summary-space 1.5))))))

