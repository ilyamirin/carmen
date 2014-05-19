(ns carmen.index
  (:use [carmen.tools]))

;;indexing

;;TODO easy find by size (find least applicable)
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

(defn put-to-free [key value]
  (dosync
    (alter free-cells-registry assoc (hash-buffer key) value)))

(defn acquire-free-cell [min-size]
  (dosync
   (let [free-cell (first (filter #(>= (:size (get % 1)) min-size) @free-cells-registry)) ;optimize - find least applicable
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