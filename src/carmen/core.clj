(ns carmen.core
  (:require [taoensso.timbre :as timbre])
  (:use [carmen.tools]
        [carmen.hands]
        [carmen.index]))

;;storage operations

;;TODO: add ttl
;;TODO: exceptions processing in Store
;;TODO: compressor function
;;TODO: add checksums
;;TODO: repair function
;;TODO: add descriptions and README
;;version 1.0
;;TODO: ciphering
;;TODO: add stats
;;TODO: add fixed cell size
;;TODO: Bloom filter
;;TODO: birthday paradox problem (large keys?)
;;TODO: drain function
;;TODO: web server

;;business protocols

(defprotocol PStore
  (persist-chunk [this key chunk-body ttl] [this key chunk-body])
  (get-chunk [this key])
  (remove-chunk [this key])
  (forget-all [this])
  (rescan [this])
  (compress [this])
  (used-space [this]))

(deftype Store [^carmen.index.PHandMemory memory
                ^carmen.hands.PHand hand]
  PStore
  (persist-chunk [this key chunk-body ttl]
    (if-not (index-contains-key? memory key)
      (let [free-meta (acquire-free memory (.capacity chunk-body))]
        (put-to-index memory
          key
          (if-not (nil? free-meta)
            (retake-in-hand hand key chunk-body ttl free-meta) ;;;mb return meta to free if smthg goes wrong?
            (take-in-hand hand key chunk-body ttl))))
      (get-from-index memory key)))

  (persist-chunk [this key chunk-body]
    (persist-chunk this key chunk-body 0))

  ;;TODO: add exists method
  (get-chunk [this key]
    (if (index-contains-key? memory key)
      (let [meta (get-from-index memory key)]
        (if-not (expired? meta)
          (give-with-hand hand meta)))))

  (remove-chunk [this key]
    (if (index-contains-key? memory key)
      (do
        (drop-with-hand hand (get-from-index memory key))
        (move-from-index-to-free memory key))))

  (forget-all [this]
    (clean-indexes memory))

  (rescan [this]
    ;;TODO: refactor this
    ;;TODO: add expired to free
    ;;TODO: add forgive before
    ;;TODO: add free expires
    (defn- load-existed-chunk-key [chunk-meta]
      (let [key (read-key hand chunk-meta)]
        (if (or (= (:status chunk-meta) Byte/MAX_VALUE) (expired? chunk-meta))
          (put-to-index memory key chunk-meta)
          (put-to-free memory key chunk-meta))
        chunk-meta))

    (locking hand
      (loop [position 0
             chunks-were-loaded 0]
        (if (= (rem chunks-were-loaded 100) 0)
          (timbre/info chunks-were-loaded "chunks were loaded."))
        (if (>= position (hand-size hand))
          chunks-were-loaded
          ;;;TODO: replace with thread macro
          (recur
            (+ position
              (:cell-size
                (load-existed-chunk-key
                  (read-meta hand position))))
            (inc chunks-were-loaded))))))

  (compress [this] nil)

  (used-space [this]
    (hand-size hand)))

(defmacro defstore [name path-to-storage]
  `(defonce ~name (Store. (create-memory) (create-hand ~path-to-storage))))
