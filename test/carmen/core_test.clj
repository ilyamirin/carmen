(ns carmen.core-test
  (:require [clojure.test :refer :all]
            [carmen.tools :refer :all]
            [carmen.core :refer :all]
            [taoensso.timbre :as timbre]))

;TODO: make test file configurable
(.delete (java.io.File. "/tmp/storage2.bin"))
(defstore test-carmen "/tmp/storage2.bin")

(deftest chunk-business-operations-test
  (testing "Test of chunks persist/read/remove operations."
    ;;;create-remove
    (let [key (create-and-fill-buffer 16)
          chunk-body (create-and-fill-buffer (rand-int 65536))]
      (persist-chunk test-carmen key chunk-body)
      (is (chunks-are-equal? (get-chunk test-carmen key) chunk-body))

      (remove-chunk test-carmen key)
      (is (nil? (get-chunk test-carmen key))))

    ;;;ttl
    ;;;do not clean index after all because we test ttl rewrite with first opertaion of the next test
    (let [key (create-and-fill-buffer 16)
          chunk-body (create-and-fill-buffer (rand-int 65536))
          index-entry (persist-chunk test-carmen key chunk-body 100)
          chunk-meta (first (vals index-entry))]
      (is (= (:status chunk-meta) 127))
      (is (= (:size chunk-meta) (.capacity chunk-body)))
      (is (= (:ttl chunk-meta) 100))
      (is (chunks-are-equal? (get-chunk test-carmen key) chunk-body))
      (Thread/sleep 100)
      (is (nil? (get-chunk test-carmen key))))))

(deftest concurrent-chunk-business-operations-test
  (testing "Concurrent test of chunks persist/read/remove operations."
    (def chunks (ref {}))
    (def removed-chunks (ref {}))

    (defn one-operation-quad []
      (let [key (create-and-fill-buffer 16)
            chunk-body (create-and-fill-buffer (rand-int 65536))]
        (persist-chunk test-carmen key chunk-body)
        (is (chunks-are-equal? (get-chunk test-carmen key) chunk-body))
        (if (> (rand) 0.5)
          (do
            (remove-chunk test-carmen key)
            (is (nil? (get-chunk test-carmen key)))
            (dosync
              (alter removed-chunks assoc key chunk-body)))
          (dosync
            (alter chunks assoc key chunk-body)))))

    (defn repeated-quad [n]
      (dorun n
        (repeatedly n one-operation-quad))
      (timbre/info "One testing thread has finished at" (System/currentTimeMillis)))

    (let [start (System/currentTimeMillis)]
      (dorun (pvalues (repeated-quad 1000) (repeated-quad 1000) (repeated-quad 1000)))
      (timbre/info (count @chunks) "chunks processed for" (- (System/currentTimeMillis) start) "mseconds"))

    (doall (map #(is (chunks-are-equal? (get-chunk test-carmen %) (get @chunks %))) (keys @chunks)))
    (doall (map #(is (not (get-chunk test-carmen %))) (keys @removed-chunks)))

    (let [key-meta-space (+ (:size-of-meta constants) (:size-of-key constants))
          summary-space (reduce #(+ %1 (.capacity %2) key-meta-space) 0 (vals @chunks))]
      (timbre/info (int (/ (used-space test-carmen) 1000000)) "Mb of space was used")
      (timbre/info (int (/ summary-space 1000000)) "Mb of data was loaded")
      (is (< (used-space test-carmen) (* summary-space 1.5))))

    (forget-all test-carmen)

    (is (>= (rescan test-carmen) (count @chunks)))

    (doall (map #(is (chunks-are-equal? (get-chunk test-carmen %) (get @chunks %))) (keys @chunks)))
    (doall (map #(is (not (get-chunk test-carmen %))) (keys @removed-chunks)))

    (let [start (System/currentTimeMillis)
          old-count (count @chunks)]
      (dorun (pvalues (repeated-quad 1000) (repeated-quad 1000) (repeated-quad 1000)))
      (timbre/info (- (count @chunks) old-count) "chunks processed for" (- (System/currentTimeMillis) start) "mseconds"))

    (doall (map #(is (chunks-are-equal? (get-chunk test-carmen %) (get @chunks %))) (keys @chunks)))
    (doall (map #(is (not (get-chunk test-carmen %))) (keys @removed-chunks)))

    (let [key-meta-space (+ (:size-of-meta constants) (:size-of-key constants))
          summary-space (reduce #(+ %1 (.capacity %2) key-meta-space) 0 (vals @chunks))]
      (timbre/info (int (/ (used-space test-carmen) 1000000)) "Mb of space was used")
      (timbre/info (int (/ summary-space 1000000)) "Mb of data was loaded")
      (is (< (used-space test-carmen) (* summary-space 1.5))))))

