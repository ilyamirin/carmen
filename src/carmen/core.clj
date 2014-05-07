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

;;indexing

(def index (atom {}))

(defn put-key-to-index [key value]
  (swap! index assoc (-> key (.clear ) (.hashCode)) value))

(defn index-contains-key? [key]
  (contains? @index (-> key (.clear ) (.hashCode))))

(defn get-key-from-index [key]
  (get @index (-> key (.clear ) (.hashCode))))

;(def key (-> (create-buffer 33) (.clear ) (.putInt 12)))
;(put-key-to-index key {:position 0})
;(index-contains-key? key)
;(get-key-from-index key)

;;storage operations
(defn wrap-buffers [& buffers]
  (let [buffer-size (reduce #(+ %1 (.capacity %2)) 0 buffers)]
    (reduce #(.put %1 (.rewind %2)) (.clear (create-buffer buffer-size)) buffers)))

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
        (put-key-to-index key chunk-meta)))

;;TODO: overwrite and read chunk method
(defn overwrite-chunk [key chunk-body chunk-meta]
  (let [buffer (wrap-buffers key chunk-body)]
    (.write get-chunk-store (.rewind buffer) (:position chunk-meta))
    (put-key-to-index key chunk-meta)))

(defn get-chunk [key]
  (let [chunk-meta (get-key-from-index key)
        chunk-body (create-buffer (:size chunk-meta))
        chunk-position (+ (:position chunk-meta) (:chunk-position-offset constants))]
    (.read get-chunk-store (.clear chunk-body) chunk-position)
    (.rewind chunk-body)))

;chunk-meta {:status false :position 12 :size 65536 }
;(def key (-> (create-buffer 16) (.clear ) (.putInt 12221)))
;(def chunk-body (-> (create-buffer 256) (.clear ) (.putInt 0 1988)))

;(-> chunk-body (.rewind ) (.hashCode ))
;(.getInt (.rewind chunk-body) 0)
;(map #(.get (.rewind chunk-body) %) (range 0 256))

;(append-chunk key chunk-body)
;(.size get-chunk-store)

;(-> (get-chunk key) (.rewind ) (.hashCode ))
;(-> (get-chunk key) (.rewind ) (.getInt 0))
;(map #(.get (get-chunk key) %) (range 0 256))

;;business methods

;;TODO write-overwrite method (add condemned registry)
(defn persist-chunk [key chunk-body]
    (if (index-contains-key? key) () ())
)

;;TODO read ing method

;;TODO whole storage loader

;;TODO testing
