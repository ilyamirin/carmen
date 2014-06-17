(ns carmen.core-test
  (:require [clojure.test :refer :all]
            [carmen.tools :refer :all]
            [carmen.core :refer :all]
            [taoensso.timbre :as timbre])
  (:import [java.nio.channels OverlappingFileLockException]))

(def test-file "./carmen_storage.bin")
(.delete (java.io.File. test-file))
(defstore test-carmen test-file)

(deftest chunk-business-operations-test
  (testing "Test of chunks persist/read/remove operations."
    (is (thrown? OverlappingFileLockException (.lock (get-channel-of-file test-file))))

    ;;;create-remove
    (let [key (create-and-fill-buffer 16)
          chunk-body (create-and-fill-buffer (rand-int 65536))]
      (persist-chunk test-carmen key chunk-body)
      (is (exists-key? test-carmen key))
      (is (chunks-are-equal? (get-chunk test-carmen key) chunk-body))

      (remove-chunk test-carmen key)
      (is (not (exists-key? test-carmen key)))
      (is (nil? (get-chunk test-carmen key))))

    ;;;ttl
    (let [key (create-and-fill-buffer 16)
          chunk-body (create-and-fill-buffer 1)
          index-entry (persist-chunk test-carmen key chunk-body 100)
          chunk-meta (first (vals index-entry))]

      (is (= (:status chunk-meta) 127))
      (is (= (:size chunk-meta) (.capacity chunk-body)))
      (is (= (:ttl chunk-meta) 100))
      (is (exists-key? test-carmen key))
      (is (chunks-are-equal? (get-chunk test-carmen key) chunk-body))

      (Thread/sleep 100)
      (is (not (exists-key? test-carmen key)))
      (is (nil? (get-chunk test-carmen key)))

      (forget-all test-carmen)
      (rescan test-carmen)
      (is (not (exists-key? test-carmen key)))
      (is (nil? (get-chunk test-carmen key)))

      ;;;expired chunk must be added to free after remember
      (let [used (:file-size (get-state test-carmen))]
        (persist-chunk test-carmen (create-and-fill-buffer 16) chunk-body)
        (is (= used (:file-size (get-state test-carmen))))))))

(deftest concurrent-chunk-business-operations-test
  (testing "Concurrent test of chunks persist/read/remove operations."
    (def chunks (ref {}))
    (def removed-chunks (ref {}))

    (defn one-operation-quad []
      (let [key (create-and-fill-buffer 16)
            chunk-body (create-and-fill-buffer (rand-int 65536))]
        (persist-chunk test-carmen key chunk-body)
        (is (exists-key? test-carmen key))
        (is (chunks-are-equal? (get-chunk test-carmen key) chunk-body))
        (if (> (rand) 0.5)
          (do
            (remove-chunk test-carmen key)
            (is (not (exists-key? test-carmen key)))
            (is (nil? (get-chunk test-carmen key)))
            (dosync
              (alter removed-chunks assoc key chunk-body)))
          (dosync
            (alter chunks assoc key chunk-body)))))

    (defn repeated-quad [n]
      (dorun n
        (repeatedly n one-operation-quad))
      (timbre/info "One testing thread has finished at" (System/currentTimeMillis)))

    (defn check-testing-results []
      (doall (map #(is (chunks-are-equal? (get-chunk test-carmen %) (get @chunks %))) (keys @chunks)))
      (doall (map #(is (not (get-chunk test-carmen %))) (keys @removed-chunks))))

    (is (.exists (java.io.File. test-file)))

    (let [start (System/currentTimeMillis)]
      (dorun
        (pvalues
          (repeated-quad 1000) (repeated-quad 1000) (repeated-quad 1000)))
      (timbre/info (count @chunks) "chunks processed for" (- (System/currentTimeMillis) start) "mseconds"))

    (let [key-meta-space (:not-chunk-size constants)
          summary-space (reduce #(+ %1 (.capacity %2) key-meta-space) 0 (vals @chunks))
          used-space (:file-size (get-state test-carmen))]
      (timbre/info (int (/ used-space 1000000)) "Mb of space was used")
      (timbre/info (int (/ summary-space 1000000)) "Mb of data was loaded")
      (is (< used-space (* summary-space 1.5))))

    (let [state (get-state test-carmen)]
      (forget-all test-carmen)
      (rescan test-carmen)
      (is (>= (rescan test-carmen) (count @chunks)))
      (is (= state (get-state test-carmen))))

    (check-testing-results )

    (let [start (System/currentTimeMillis)
          old-count (count @chunks)]
      (dorun (pvalues (repeated-quad 1000) (repeated-quad 1000) (repeated-quad 1000)))
      (timbre/info (- (count @chunks) old-count) "chunks processed for" (- (System/currentTimeMillis) start) "mseconds"))

    (check-testing-results )

    (let [key-meta-space (+ (:size-of-meta constants) (:size-of-key constants))
          summary-space (reduce #(+ %1 (.capacity %2) key-meta-space) 0 (vals @chunks))
          used-space (:file-size (get-state test-carmen))]
      (timbre/info (int (/ used-space 1000000)) "Mb of space was used")
      (timbre/info (int (/ summary-space 1000000)) "Mb of data was loaded")
      (is (< used-space (* summary-space 1.5))))))

