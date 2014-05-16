(ns carmen.core
  (:import
    [java.net InetSocketAddress]
    [java.nio ByteBuffer]
    [java.nio.channels ServerSocketChannel Selector SelectionKey]
    [java.nio.charset Charset]
    [java.io File RandomAccessFile]))

(def constants {:size-of-meta 13 :size-of-key 16 :chunk-position-offset 29})

;;tools

;TODO: add buffers pool
(defn create-buffer [capacity]
  (ByteBuffer/allocate capacity))

(defn buffer-to-seq [buffer]
  (map #(.get (.rewind buffer) %) (range 0 (.capacity buffer))))

(defn hash-buffer [buffer]
  (-> buffer (.rewind ) (.hashCode)))

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

;;indexing

;;TODO: Bloom filter
;;TODO: birthday paradox??

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

;;TODO: test this
(defn put-to-free [key value]
  (dosync
    (alter free-cells-registry assoc (hash-buffer key) value)))

(defn acquire-free-cell [min-size]
  (dosync
   (let [free-cell (first (filter #(>= (:size (get % 1)) min-size) @free-cells-registry))
         key-hash (first free-cell)
         chunk-meta (get @free-cells-registry key-hash)]
     (if-not (nil? free-cell)
       (do
         (alter free-cells-registry dissoc key-hash)
         (alter locked-free-cells-registry assoc key-hash chunk-meta)
         chunk-meta)))))

(defn finalize-key [key]
  (dosync
   (alter locked-free-cells-registry dissoc (hash-buffer key))))

;;storage operations

;;TODO: Ciphering
;;TODO: add while .
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

;;TODO: overwrite and read chunk method
(defn overwrite-chunk [key chunk-body chunk-meta];make getting from index
  (let [buffer (wrap-buffers key chunk-body)
        position (+ (:position chunk-meta) (:size-of-meta constants))]
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

;;TODO whole storage loader - gest this trash

(defn buffer-to-meta [buffer]
  {:status (.get buffer 0)
   :position (.getLong buffer 1)
   :size (.getInt buffer 9)})

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

(load-existed-chunk-key (load-existed-chunk-meta 0))

(reset-chunk-store)

(let [key (-> (create-buffer 16) (.clear) (.putInt 0 (rand-int 65536)))
      chunk-body (-> (create-buffer 256) (.clear) (.putInt 0 (rand-int 65536)))
      key1 (-> (create-buffer 16) (.clear) (.putInt 0 (rand-int 65536)))
      chunk-body1 (-> (create-buffer 256) (.clear) (.putInt 0 (rand-int 65536)))]

  (persist-chunk key chunk-body))

(load-existed-chunk-key (load-existed-chunk-meta 0))
(clean-indexes)

(defn load-whole-existed-storage []
  (loop [position 0]
    (if (>= position (.size get-chunk-store))
      true
      (recur
        (+ position
          (:size (load-existed-chunk-key (load-existed-chunk-meta position)))
          (:chunk-position-offset constants))))))

(load-whole-existed-storage)
@index

;;TODO compressor function

;;TODO web server
