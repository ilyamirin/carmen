(ns carmen.tools
  (:require [taoensso.timbre :as timbre])
  (:import [java.nio ByteBuffer]
           [java.io RandomAccessFile]))

(def constants {:size-of-meta 29
                :size-of-key 16
                :chunk-position-offset 45
                :size-of-checksum 4
                :not-chunk-size 49})

;;tools

;TODO: add buffers pool
(defn create-buffer [capacity]
  (ByteBuffer/allocate capacity))

(defn create-and-fill-buffer [capacity]
  (let [array (byte-array capacity)
        r (java.util.Random.)]
    (.nextBytes r array)
    (ByteBuffer/wrap array)))

(defn buffer-to-seq [buffer]
  (map #(.get (.rewind buffer) %) (range 0 (.capacity buffer))))

(defn hash-buffer [buffer]
  (-> buffer (.rewind) (.hashCode)))

(defn hash-buffers [& buffers]
  (reduce #(bit-xor %1 (hash-buffer %2)) Integer/MAX_VALUE buffers))

(defn wrap-chunk-meta [chunk-meta]
  (-> (create-buffer (:size-of-meta constants))
    (.put 0 (:status chunk-meta))
    (.putLong 1 (:position chunk-meta))
    (.putInt 9 (:size chunk-meta))
    (.putInt 13 (:cell-size chunk-meta))
    (.putLong 17 (:born chunk-meta))
    (.putInt 25 (:ttl chunk-meta))))

(defn wrap-key-chunk-and-meta [key chunk-body chunk-meta]
  (let [chunk-meta-buffer (wrap-chunk-meta chunk-meta)
        checksum-position (+ (:chunk-position-offset constants) (.capacity chunk-body))
        size (+ checksum-position (:size-of-checksum constants))
        checksum (hash-buffers key chunk-body chunk-meta-buffer)]
    (-> (create-buffer size)
      .clear
      (.put (.rewind chunk-meta-buffer))
      (.put (.rewind key))
      (.put (.rewind chunk-body))
      (.putInt checksum-position checksum)
      .rewind)))

(defn buffer-to-meta [buffer]
  {:status (.get buffer 0)
   :position (.getLong buffer 1)
   :size (.getInt buffer 9)
   :cell-size (.getInt buffer 13)
   :born (.getLong buffer 17)
   :ttl (.getInt buffer 25)})

(defn get-channel-of-file [filepath]
  (.getChannel (RandomAccessFile. filepath "rw")))

(defn chunks-are-equal? [^java.nio.ByteBuffer chunk1
                         ^java.nio.ByteBuffer chunk2]
  (= (hash-buffer chunk1) (hash-buffer chunk2)))

;;TODO: throw exception if unknown consistency was got
;;TODO: quorum must return before all operations would complete
(defmacro apply-consistently [consistency fname coll & args]
  `(let [result# (doall (map #(~fname % ~@args) ~coll))]
     (case ~consistency
       :one (not= true (every? false? result#))
       :quorum (let [quorum# (+ (bit-shift-right (count ~coll) 1) 1)]
                 (<= quorum#
                   (reduce #(if (true? %2) (inc %1) %1) 0 result#)))
       :all (every? true? result#))))

(defn expired? [meta]
  (let [ttl (:ttl meta)
        born (:born meta)]
    (if (pos? ttl)
      (< (+ born ttl) (System/currentTimeMillis ))
      false)))