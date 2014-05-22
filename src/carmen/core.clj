(ns carmen.core
  (:use [carmen.tools]
        [carmen.index])
  (:import    
    [java.io RandomAccessFile]))

;;TODO add descriptions and README

;;storage operations

;;TODO: ciphering
;;TODO: large keys?
;;TODO: exceptions processing
;;TODO: add multy storage support

(def get-chunk-store (.getChannel (new RandomAccessFile "/tmp/storage.bin" "rw")))

(defn reset-chunk-store []
  (.truncate get-chunk-store 0))

(defn- append-chunk [key chunk-body]
  (locking get-chunk-store
    (let [position (.size get-chunk-store)
          chunk-meta {:status Byte/MAX_VALUE :position position :size (.capacity chunk-body)}
          buffer (wrap-key-chunk-and-meta key chunk-body chunk-meta)]
      (while (.hasRemaining buffer)
        (.write get-chunk-store buffer position))
      (put-to-index key chunk-meta)
      chunk-meta)))

(defn- overwrite-chunk [key chunk-body free-cell]
  (let [free-key (first free-cell)
        position (:position (second free-cell))
        chunk-meta {:status Byte/MAX_VALUE :position position :size (.capacity chunk-body)}
        buffer (wrap-key-chunk-and-meta key chunk-body chunk-meta)]
    (locking get-chunk-store
      (while (.hasRemaining buffer)
        (.write get-chunk-store buffer position)))
    (put-to-index key chunk-meta)
    (finalize-free-cell free-key)))

(defn- read-chunk [key]
  (let [chunk-meta (get-from-index key)
        chunk-body (.clear (create-buffer (:size chunk-meta)))
        chunk-position (+ (:position chunk-meta) (:chunk-position-offset constants))]
    (locking get-chunk-store
      (while (.hasRemaining chunk-body)
        (.read get-chunk-store chunk-body chunk-position)))
    (.rewind chunk-body)))

(defn- kill-chunk [key]
  (let [position (:position (get-from-index key))
        buffer (.rewind (.put (create-buffer 1) 0 Byte/MIN_VALUE))]
    (locking get-chunk-store
      (while (.hasRemaining buffer)
        (.write get-chunk-store buffer position))))
  (move-from-index-to-free key))

;;business methods

(defn persist-chunk [key chunk-body]
  (if-not (index-contains-key? key)
    (let [free-cell (acquire-free-cell (.capacity chunk-body))]
      (if-not (nil? free-cell)
        (overwrite-chunk key chunk-body free-cell)
        (append-chunk key chunk-body)))
    (get-from-index key)))

(defn get-chunk [key]
  (if (index-contains-key? key)
    (read-chunk key)))

(defn remove-chunk [key]
  (if (index-contains-key? key) (kill-chunk key)))

;;existed storage processing

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
          (:size (load-existed-chunk-key
                   (load-existed-chunk-meta position)))
          (:chunk-position-offset constants))))))

;;TODO compressor function
(defn compress-storage [] 
  )

;;TODO web server
