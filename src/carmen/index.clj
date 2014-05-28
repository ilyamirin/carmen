(ns carmen.index
  (:use [carmen.tools]))

;;indexing

;TODO: make it as type
;;TODO: optimize finding by size
;;TODO: Bloom filter
;;TODO: birthday paradox problem (large keys?)

;;TODO make this finding more intellectual
(defn- find-first-applicable-cell [min-size registry]
  (first (filter #(>= (:size (get % 1)) min-size) registry)))

(defprotocol PHandMemory
  (clean-indexes [this])
  (put-to-index [this key value])
  (index-contains-key? [this key])
  (get-from-index [this key])
  (move-from-index-to-free [this key])
  (put-to-free [this key value])
  (acquire-free [this min-size]))

(deftype HandMemory [^clojure.lang.Ref index
                     ^clojure.lang.Ref free-cells]
  PHandMemory
  (clean-indexes [this]
    (dosync
      (doall
        (map #(ref-set % {}) [index free-cells]))))

  (put-to-index [this key value]
    (dosync
      (alter index assoc (hash-buffer key) value)))

  (index-contains-key? [this key]
    (contains? @index (hash-buffer key)))

  (get-from-index [this key]
    (get @index (hash-buffer key)))

  (move-from-index-to-free [this key]
    (dosync
      (let [key-hash (hash-buffer key)]
        (alter free-cells assoc key-hash (get @index key-hash))
        (alter index dissoc key-hash))))

  (put-to-free [this key value]
    (dosync
      (alter free-cells assoc (hash-buffer key) value)))

  (acquire-free [this min-size]
    (dosync
      (let [free-cell (find-first-applicable-cell min-size @free-cells)]
        (if-not (nil? free-cell)
          (let [key-hash (first free-cell)
                chunk-meta (second free-cell)]
            (alter free-cells dissoc key-hash)
            chunk-meta))))))

(defn create-memory []
  (HandMemory. (ref {}) (ref {})))