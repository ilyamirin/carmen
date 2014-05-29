(ns carmen.core-test
  (:require [clojure.test :refer :all]
            [carmen.tools :refer :all]
            [carmen.core :refer :all]))

(deftest concurrent-chunk-business-operations-test
  (testing "Concurrent test of chunks persist/read/remove operations."
    (.delete (java.io.File. "/tmp/storage2.bin"))

    (defstore test-carmen "/tmp/storage2.bin")

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
      (println "One testing thread has finished at" (System/currentTimeMillis)))

    (let [start (System/currentTimeMillis)]
      (dorun (pvalues (repeated-quad 1000) (repeated-quad 1000) (repeated-quad 1000)))
      (println (count @chunks) "chunks processed for" (- (System/currentTimeMillis) start) "mseconds"))

    (doall (map #(is (chunks-are-equal? (get-chunk test-carmen %) (get @chunks %))) (keys @chunks)))
    (doall (map #(is (not (get-chunk test-carmen %))) (keys @removed-chunks)))

    (let [key-meta-space (+ (:size-of-meta constants) (:size-of-key constants))
          summary-space (reduce #(+ %1 (.capacity %2) key-meta-space) 0 (vals @chunks))]
      (println (int (/ summary-space 1000000)) "Mb of space was used")
      (is (< (used-space test-carmen) (* summary-space 1.5))))

    (forget-all test-carmen)

    (is (>= (rescan test-carmen) (count @chunks)))

    (doall (map #(is (chunks-are-equal? (get-chunk test-carmen %) (get @chunks %))) (keys @chunks)))
    (doall (map #(is (not (get-chunk test-carmen %))) (keys @removed-chunks)))

    (let [start (System/currentTimeMillis)
          old-count (count @chunks)]
      (dorun (pvalues (repeated-quad 1000) (repeated-quad 1000) (repeated-quad 1000)))
      (println (- (count @chunks) old-count) "chunks processed for" (- (System/currentTimeMillis) start) "mseconds"))

    (doall (map #(is (chunks-are-equal? (get-chunk test-carmen %) (get @chunks %))) (keys @chunks)))
    (doall (map #(is (not (get-chunk test-carmen %))) (keys @removed-chunks)))

    (let [key-meta-space (+ (:size-of-meta constants) (:size-of-key constants))
          summary-space (reduce #(+ %1 (.capacity %2) key-meta-space) 0 (vals @chunks))]
      (println (int (/ summary-space 1000000)) "Mb of space was used")
      (is (< (used-space test-carmen) (* summary-space 1.5))))))

