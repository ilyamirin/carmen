(ns carmen.core
  (:require [taoensso.timbre :as timbre])
  (:use [carmen.tools]
        [carmen.hands]
        [carmen.index]))

;;storage operations

;;TODO: add checksums
;;TODO: repair function (remove incorrect, kill expired)
;;TODO: pour function
;;TODO: add fixed cell size
;;TODO: add descriptions and README
;;version 1.0
;;TODO: ciphering
;;TODO: add stats
;;TODO: Bloom filter
;;TODO: birthday paradox problem (large keys?)
;;TODO: drain function
;;TODO: add consistency executions
;;TODO: add consistent key maps
;;TODO: web server

;;business protocols

(defprotocol PStore
  (persist-chunk [this key chunk-body ttl] [this key chunk-body])
  (get-chunk [this key])
  (remove-chunk [this key])
  (forget-all [this])
  (rescan [this fn])
  (remember-all [this])
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

  (rescan [this fn]
    (locking hand
      (loop [position 0
             chunks-were-scanned 0]
        (if (= (rem chunks-were-scanned 100) 0)
          (timbre/info chunks-were-scanned "chunks were scanned."))
        (if (>= position (hand-size hand))
          (do
            (timbre/info chunks-were-scanned "chunks were scanned.")
            chunks-were-scanned)
          (recur
            (->> position (read-meta hand ) fn :cell-size (+ position))
            (inc chunks-were-scanned))))))

  (remember-all [this]
    (timbre/info "Start remembering.")
    (letfn [(load-existed-chunk-key [chunk-meta]
              (let [key (read-key hand chunk-meta)]
                (if (or (= (:status chunk-meta) Byte/MIN_VALUE) (expired? chunk-meta))
                  (put-to-free memory key chunk-meta)
                  (put-to-index memory key chunk-meta))
                chunk-meta))]
      (rescan this load-existed-chunk-key)))

  (used-space [this]
    (hand-size hand)))

(defmacro defstore [name path-to-storage]
  `(defonce ~name (Store. (create-memory) (create-hand ~path-to-storage))))
