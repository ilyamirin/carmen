(ns carmen.core
  (:use [carmen.tools]
        [carmen.index])
  (:import    
    [java.io RandomAccessFile]))

;;TODO add descriptions and README

;;storage operations

;;TODO: ciphering
;;TODO: add while hasRemaining
;;TODO: add multy storage support

(def get-chunk-store (.getChannel (new RandomAccessFile "/tmp/storage.bin" "rw")))

(defn reset-chunk-store []
  (.truncate get-chunk-store 0))

(defn append-chunk [key chunk-body]
  (let [position (.size get-chunk-store)
        chunk-meta {:status Byte/MAX_VALUE :position position :size (.capacity chunk-body)}
        buffer (wrap-key-chunk-and-meta key chunk-body chunk-meta)]
        (.write get-chunk-store (.rewind buffer) position)
        (put-to-index key chunk-meta)))

(defn overwrite-chunk [key chunk-body chunk-meta];make getting from index
  (let [buffer (wrap-key-chunk-and-meta key chunk-body (assoc chunk-meta :status Byte/MAX_VALUE))
        position (:position chunk-meta)]
    (.write get-chunk-store (.rewind buffer) position)
    (put-to-index key chunk-meta)))

(defn read-chunk [key]
  (let [chunk-meta (get-from-index key)
        chunk-body (create-buffer (:size chunk-meta))
        chunk-position (+ (:position chunk-meta) (:chunk-position-offset constants))]
    (.read get-chunk-store (.clear chunk-body) chunk-position)
    (.rewind chunk-body)))

(defn kill-chunk [key]
  (let [position (:position (get-from-index key))
        buffer (.put (create-buffer 1) 0 Byte/MIN_VALUE)]
    (.write get-chunk-store (.clear buffer) position))
  (move-from-index-to-free key))

;;business methods

(defn persist-chunk [key chunk-body]
  (if-not (index-contains-key? key)
    (if (not-empty @free-cells-registry)
      (overwrite-chunk key chunk-body (acquire-free-cell (.capacity chunk-body)))
      (append-chunk key chunk-body))
    (get-from-index key)))

(defn get-chunk [key]
  (if (index-contains-key? key)
    (read-chunk key)))

(defn remove-chunk [key]
  (if (index-contains-key? key) (kill-chunk key)))

;;TODO splite code to 4 NS
;;TODO whole storage loader - gest this trash

(defn load-existed-chunk-meta [position]
  (let [meta-buffer (create-buffer (:size-of-meta constants))]
    (.read get-chunk-store (.clear meta-buffer) position)
    (buffer-to-meta meta-buffer)))

(defn load-existed-chunk-key [chunk-meta]
  (let [key-buffer (create-buffer (:size-of-key constants))
        position (+ (:position chunk-meta) (:size-of-meta constants))]
    (.read get-chunk-store (.clear key-buffer) position)
    (if (= (:status chunk-meta) Byte/MAX_VALUE)
      (put-to-index key-buffer chunk-meta)
      (put-to-free key-buffer chunk-meta))
    chunk-meta))

(defn load-whole-existed-storage []
  (loop [position 0]
    (if (>= position (.size get-chunk-store))
      true
      (recur
        (+ position
          (:size (load-existed-chunk-key (load-existed-chunk-meta position)))
          (:chunk-position-offset constants))))))

;;TODO compressor function
(defn compress-storage [] 
  )

;;TODO web server
