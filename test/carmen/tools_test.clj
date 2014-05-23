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