(ns carmen.core
  (:require [taoensso.timbre :as timbre])
  (:use [carmen.tools]
        [carmen.hands]
        [carmen.index]))

;;storage operations

;;TODO: mutate repair to pour function (as compact) + hand state
;;TODO: check expired in index function
;;TODO: add fixed cell size option
;;TODO: add overwriting option
;;TODO: switch to channels
;;TODO: add descriptions and README with usecases
;;version 1.0
;;TODO: fix test descriptions and split huge tests
;;TODO: return unused acquired cells to free
;;TODO: ciphering
;;TODO: add states
;;TODO: Bloom filter
;;TODO: birthday paradox problem (large keys?)
;;TODO: drain function??
;;TODO: add consistency executions
;;TODO: add consistent key maps
;;TODO: web server

;;business protocols

(defprotocol PStore
  (persist-chunk [this key chunk-body ttl] [this key chunk-body])
  (exists-key? [this key])
  (get-chunk [this key])
  (remove-chunk [this key])
  (get-state [this])
  (forget-all [this])
  (apply-fn-to-every-chunk [this fn])
  (rescan [this]))

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

  (exists-key? [this key]
    (if (index-contains-key? memory key)
      (let [meta (get-from-index memory key)]
        (if-not (expired? meta) true false))
      false))

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

  (get-state [this]
    {:file-size (hand-size hand)
     :memory (get-memory-state memory)})

  ;;;do not call therefore functions durung work

  (forget-all [this]
    (clean-indexes memory))

  (apply-fn-to-every-chunk [this fn]
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

  (rescan [this]
    (timbre/info "Start rescan.")
    (letfn [(load-existed-chunk-key [chunk-meta]
              (let [key (read-key hand chunk-meta)
                    not-holistic? (not (holistic? hand chunk-meta))
                    deleted? (= (:status chunk-meta) Byte/MIN_VALUE)
                    old? (expired? chunk-meta)]
                (if (or not-holistic? deleted? old?)
                  (put-to-free memory key chunk-meta)
                  (put-to-index memory key chunk-meta))
                (if (or not-holistic? old?)
                  (drop-with-hand hand chunk-meta)
                  chunk-meta)))]
      (apply-fn-to-every-chunk this load-existed-chunk-key))))

(defmacro defstore [name path-to-storage]
  `(defonce ~name (Store. (create-memory) (create-hand ~path-to-storage))))