(ns carmen.tools
  (:import [java.nio ByteBuffer]))

(def constants {:size-of-meta 17 :size-of-key 16 :chunk-position-offset 33})

;;tools

;TODO: add buffers pool
(defn create-buffer [capacity]
  (ByteBuffer/allocate capacity))

(defn buffer-to-seq [buffer]
  (map #(.get (.rewind buffer) %) (range 0 (.capacity buffer))))

(defn hash-buffer [buffer]
  (-> buffer (.rewind) (.hashCode)))

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
      (.putInt 13 (:cell-size chunk-meta))
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
   :cell-size (.getInt buffer 13)})

