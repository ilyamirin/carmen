(ns carmen.core
  (:import
    [java.net InetSocketAddress]
    [java.nio ByteBuffer]
    [java.nio.channels ServerSocketChannel Selector SelectionKey]
    [java.nio.charset Charset]
    [java.io RandomAccessFile]))

(def constants {:size-of-meta 13 :size-of-key 16 :chunk-position-offset 29})

;;tools

(defn create-buffer [capacity]
  (ByteBuffer/allocate capacity))

(defn buffer-to-seq [buffer]
  (map #(.get (.rewind buffer) %) (range 0 (.capacity buffer))))

(defn hash-buffer [buffer]
  (-> buffer (.rewind ) (.hashCode)))

(defn wrap-buffers [& buffers]
  (let [buffer-size (reduce #(+ %1 (.capacity %2)) 0 buffers)]
    (reduce #(.put %1 (.rewind %2)) (.clear (create-buffer buffer-size)) buffers)))

;;indexing

;;TODO: birthday paradox

(def index (ref {}))
(def free-cells-registry (ref {}))
(def locked-free-cells-registry (ref {}))

(defn clean-indexes []
  (dosync
   (doall
     (map #(ref-set % {}) [index free-cells-registry locked-free-cells-registry]))))

(defn put-to-index [key value]
  (dosync
   (alter index assoc (hash-buffer key) value)))

(defn index-contains-key? [key]
  (contains? @index (hash-buffer key)))

(defn get-from-index [key]
  (get @index (hash-buffer key)))

(defn move-from-index-to-free [key]
  (dosync
   (let [key-hash (hash-buffer key)
         chunk-meta (get @index key-hash)]
     (alter free-cells-registry assoc key-hash chunk-meta)
     (alter index dissoc key-hash))))

(defn acquire-free-cell []
  (dosync
   (let [key-hash (first (first @free-cells-registry))
         chunk-meta (get @free-cells-registry key-hash)]
     (alter free-cells-registry dissoc key-hash)
     (alter locked-free-cells-registry assoc key-hash chunk-meta)
     chunk-meta)))

(defn finalize-key [key]
  (dosync
   (alter locked-free-cells-registry dissoc (hash-buffer key))))

;;@index
;(def key (-> (create-buffer 33) (.clear ) (.putInt 1)))
;(put-to-index key {:position 21})
;(index-contains-key? key)
;(get-from-index key)
;@index
;(clean-indexes )
;@index
;(move-from-index-to-free key)
;(acquire-free-cell )
;(finalize-key key)

;;storage operations
(defn wrap-key-chunk-and-meta [key chunk-body chunk-meta]
  (let [capacity (+ (:size-of-meta constants) (:size-of-key constants) (.capacity chunk-body))
        chunk-meta-buffer (create-buffer (:size-of-meta constants))]
    (-> chunk-meta-buffer
        (.put 0 (:status chunk-meta))
        (.putLong 1 (:position chunk-meta))
        (.putInt 9 (:size chunk-meta))
        (.rewind ))
    (-> (create-buffer capacity)
        (.clear )
        (.put chunk-meta-buffer)
        (.put (.rewind key))
        (.put (.rewind chunk-body)))))

(def get-chunk-store (.getChannel (new RandomAccessFile "/tmp/storage.bin" "rw")))

(defn append-chunk [key chunk-body]
  (let [position (.size get-chunk-store)
        chunk-meta {:status Byte/MAX_VALUE :position position :size (.capacity chunk-body)}
        buffer (wrap-key-chunk-and-meta key chunk-body chunk-meta)]
        (.write get-chunk-store (.rewind buffer) position)
        (put-to-index key chunk-meta)))

;;TODO: overwrite and read chunk method
(defn overwrite-chunk [key chunk-body chunk-meta];make getting from index
  (let [buffer (wrap-buffers key chunk-body)
        position (+ (:position chunk-meta) (:size-of-meta constants))]
    (.write get-chunk-store (.rewind buffer) position)
    (put-to-index key chunk-meta)))

(defn get-chunk [key]
  (let [chunk-meta (get-from-index key)
        chunk-body (create-buffer (:size chunk-meta))
        chunk-position (+ (:position chunk-meta) (:chunk-position-offset constants))]
    (.read get-chunk-store (.clear chunk-body) chunk-position)
    (.rewind chunk-body)))

;chunk-meta {:status false :position 12 :size 65536 }
;(def key (-> (create-buffer 16) (.clear ) (.putInt 121)))
;(def chunk-body (-> (create-buffer 256) (.clear ) (.putInt 0 88)))

;(-> chunk-body (.rewind ) (.hashCode ))
;(.getInt (.rewind chunk-body) 0)
;(map #(.get (.rewind chunk-body) %) (range 0 256))

;(append-chunk key chunk-body)
;(.size get-chunk-store)

;(overwrite-chunk key chunk-body {:status 127, :position 285, :size 256})

;(-> (get-chunk key) (.rewind ) (.hashCode ))
;(-> (get-chunk key) (.rewind ) (.getInt 0))
;(map #(.get (get-chunk key) %) (range 0 256))

;;business methods
;;TODO write-overwrite method (add condemned registry)
(defn persist-chunk [key chunk-body]
  (if-not (index-contains-key? key)
    (if (not-empty free-cells-registry)
      (overwrite-chunk key chunk-body (acquire-free-cell ))
      (append-chunk key chunk-body))
    (get-from-index key)))

;;TODO read ing method

;;TODO whole storage loader

;;TODO testing

;;TODO compressor function
