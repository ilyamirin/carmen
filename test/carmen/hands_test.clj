(ns carmen.hands-test
  (:require [clojure.test :refer :all]
            [carmen.tools :refer :all]
            [carmen.hands :refer :all]))

(deftest read-write-remove-hand-test
  (testing "Test read/write/remove operations with a help of Hand."
    (defhand test-hand "./test_store.bin")
    (wash-hand test-hand)

    (def saved-chunks (atom {}))
    (def test-hand-size (atom 0))

    (defn test-inner-fn []
      (let [key (create-and-fill-buffer 16)
            chunk-body (create-and-fill-buffer (rand-int 65536))
            chunk-meta (take-in-hand test-hand key chunk-body)]

        (is (= {:status Byte/MAX_VALUE
                :size (.capacity chunk-body)
                :cell-size (+ (:chunk-position-offset constants) (.capacity chunk-body))
                :position (:position chunk-meta)}
              chunk-meta))

        (is (= (hash-buffer (give-with-hand test-hand chunk-meta)) (hash-buffer chunk-body)))

        (if (> (rand) 0.5)
          (let [new-key (create-and-fill-buffer 16)
                new-chunk-body (create-and-fill-buffer (rand-int (.capacity chunk-body)))
                deleted-meta (drop-with-hand test-hand chunk-meta)
                new-meta (retake-in-hand test-hand new-key new-chunk-body deleted-meta)]
            (is (= Byte/MIN_VALUE (:status deleted-meta)))
            (is (not= deleted-meta new-meta))
            (swap! saved-chunks assoc new-meta new-chunk-body))
          (swap! saved-chunks assoc chunk-meta chunk-body))
        (swap! test-hand-size + (.capacity chunk-body) (:chunk-position-offset constants))))

    (dorun 1000
      (repeatedly 1000
        #(doall (pvalues (test-inner-fn ) (test-inner-fn ) (test-inner-fn )))))

    (doall
      (map
        #(= (hash-buffer (give-with-hand test-hand %))
           (hash-buffer (get @saved-chunks %)))
        (keys @saved-chunks)))

    (is (= @test-hand-size (hand-size test-hand)))))