(ns carmen.index
  (:use [carmen.tools]))

;;indexing

;;TODO: optimize finding by size
;;TODO: Bloom filter
;;TODO: birthday paradox problem (large keys?)

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
   (let [key-hash (hash-buffer key)]
     (alter free-cells-registry assoc key-hash (get @index key-hash))
     (alter index dissoc key-hash))))

(defn put-to-free [key value]
  (dosync
    (alter free-cells-registry assoc (hash-buffer key) value)))

(defn- find-the-least-applicable-cell [min-size registry]
  (first (filter #(>= (:size (get % 1)) min-size) registry)))

(defn acquire-free-cell [min-size]
  (dosync
   (let [free-cell (find-the-least-applicable-cell min-size @free-cells-registry)]
     (if-not (nil? free-cell)
      (let [key-hash (first free-cell)
            chunk-meta (second free-cell)]
         (alter free-cells-registry dissoc key-hash)
         (alter locked-free-cells-registry assoc key-hash chunk-meta)
         free-cell)))))

(defn finalize-free-cell [free-key]
  (dosync
    (alter locked-free-cells-registry dissoc free-key)))