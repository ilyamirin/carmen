(ns carmen.tools
  (:require [taoensso.timbre :as timbre])
  (:import [java.nio ByteBuffer]
           [java.io RandomAccessFile]))

(def constants {:size-of-meta 29 :size-of-key 16 :chunk-position-offset 45})

;;tools

;TODO: add buffers pool
(defn create-buffer [capacity]
  (ByteBuffer/allocate capacity))

(defn create-and-fill-buffer [capacity]
  (let [array (byte-array capacity)]
    (.nextBytes (java.util.Random.) array)
    (ByteBuffer/wrap array)))

(defn buffer-to-seq [buffer]
  (map #(.get (.rewind buffer) %) (range 0 (.capacity buffer))))

(defn hash-buffer [buffer]
  (-> buffer (.rewind) (.hashCode)))

(defn wrap-key-chunk-and-meta [key chunk-body chunk-meta]
  (let [capacity (+ (:size-of-meta constants) (:size-of-key constants) (.capacity chunk-body))
        chunk-meta-buffer (create-buffer (:size-of-meta constants))]
    (-> chunk-meta-buffer
      (.put 0 (:status chunk-meta))
      (.putLong 1 (:position chunk-meta))
      (.putInt 9 (:size chunk-meta))
      (.putInt 13 (:cell-size chunk-meta))
      (.putLong 17 (:born chunk-meta))
      (.putInt 25 (:ttl chunk-meta))
      (.rewind))
    (-> (create-buffer capacity)
      (.clear)
      (.put chunk-meta-buffer)
      (.put (.rewind key))
      (.put (.rewind chunk-body))
      (.rewind))))

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