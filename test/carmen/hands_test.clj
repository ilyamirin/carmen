(ns carmen.hands-test
  (:require [clojure.test :refer :all]
            [carmen.tools :refer :all]
            [carmen.hands :refer :all]))

(def test-file "./carmen_storage.bin")
(.delete (java.io.File. test-file))
(defonce test-hand (create-hand test-file))

(deftest read-write-remove-hand-test
  (testing "Test read/write/remove operations with a help of Hand."
    (is (thrown? Exception (create-hand test-file)))

    (wash-hand test-hand)
    (is (zero? (hand-size test-hand)))

    (let [key (create-and-fill-buffer 16)
          chunk-body (create-and-fill-buffer (rand-int 65536))
          chunk-meta (take-in-hand test-hand key chunk-body 1500)];;; keep it 1,5 seconds
      (is (= {:status Byte/MAX_VALUE
              :size (.capacity chunk-body)
              :cell-size (+ (:not-chunk-size constants) (.capacity chunk-body))
              :position (:position chunk-meta)
              :ttl 1500}
            (dissoc chunk-meta :born)))
      (is (not= (:born chunk-meta) nil))
      (is (> (:born chunk-meta) 0))
      (is (chunks-are-equal? chunk-body (give-with-hand test-hand chunk-meta)))
      (is (holistic? test-hand chunk-meta))

      (let [new-key (create-and-fill-buffer 16)
            new-chunk-body (create-and-fill-buffer (rand-int (.capacity chunk-body)))
            deleted-meta (drop-with-hand test-hand chunk-meta)
            new-meta (retake-in-hand test-hand new-key new-chunk-body deleted-meta)]
        (is (= Byte/MIN_VALUE (:status deleted-meta)))
        (is (not= (dissoc deleted-meta :born) (dissoc new-meta :born)))
        (is (not= (:born deleted-meta) (:born new-meta)))
        (is (holistic? test-hand new-meta))))))

(deftest concurrent-read-write-remove-hand-test
  (testing "Concurrently test read/write/remove operations with a help of Hand."
    (wash-hand test-hand)
    (is (zero? (hand-size test-hand)))

    (def saved-chunks (atom {}))
    (def test-hand-size (atom 0))

    (defn test-inner-fn []
      (let [key (create-and-fill-buffer 16)
            chunk-body (create-and-fill-buffer (rand-int 65536))
            chunk-meta (take-in-hand test-hand key chunk-body)]

        (is (= {:status Byte/MAX_VALUE
                :size (.capacity chunk-body)
                :cell-size (+ (:not-chunk-size constants) (.capacity chunk-body))
                :position (:position chunk-meta)
                :ttl 0}
              (dissoc chunk-meta :born)))

        (is (not= (:position chunk-meta) nil))
        (is (not= (:born chunk-meta) nil))
        (is (> (:born chunk-meta) 0))

        (is (= (hash-buffer (give-with-hand test-hand chunk-meta)) (hash-buffer chunk-body)))

        (if (> (rand) 0.5)
          (let [new-key (create-and-fill-buffer 16)
                new-chunk-body (create-and-fill-buffer (rand-int (.capacity chunk-body)))
                deleted-meta (drop-with-hand test-hand chunk-meta)
                new-meta (retake-in-hand test-hand new-key new-chunk-body deleted-meta)]
            (is (= Byte/MIN_VALUE (:status deleted-meta)))
            (is (not= (dissoc deleted-meta :born) (dissoc new-meta :born)))
            (swap! saved-chunks assoc new-meta new-chunk-body))
          (swap! saved-chunks assoc chunk-meta chunk-body))
        (swap! test-hand-size + (:not-chunk-size constants) (.capacity chunk-body))))

    (dorun 1000
      (repeatedly 1000
        #(doall (pvalues (test-inner-fn ) (test-inner-fn ) (test-inner-fn )))))

    (doall
      (map
        #(= (hash-buffer (give-with-hand test-hand %))
           (hash-buffer (get @saved-chunks %)))
        (keys @saved-chunks)))

    (is (= @test-hand-size (hand-size test-hand)))))