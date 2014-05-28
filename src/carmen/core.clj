(ns carmen.core
  (:use [carmen.tools]
        [carmen.hands]
        [carmen.index]))

;;TODO add descriptions and README

;;storage operations

;;TODO: make storage configurable
;;TODO: add multy storage support (Carmen proxy)
;;TODO: add checksums
;;TODO: add logger
;;TODO: add config
;;TODO: compressor function
;;TODO: ciphering
;;TODO: large keys?
;;TODO: add checksums
;;TODO: exceptions processing
;;TODO: drain function
;;TODO: web server

;;business protocols

(defprotocol PCarmen
  (persist-chunk [this key chunk-body])
  (get-chunk [this key])
  (remove-chunk [this key])
  (forget-all [this])
  (rescan [this])
  (compress [this]))

(deftype Carmen [^carmen.index.PHandMemory memory
                 ^carmen.hands.PHand hand]
  PCarmen
  (persist-chunk [this key chunk-body]
    (if-not (index-contains-key? memory key)
      (let [free-meta (acquire-free memory (.capacity chunk-body))]
        (put-to-index memory
          key
          (if-not (nil? free-meta)
            (retake-in-hand hand key chunk-body free-meta) ;;;mb return meta to free if smthg goes wrong?
            (take-in-hand hand key chunk-body))))
      (get-from-index memory key)))

  (get-chunk [this key]
    (if (index-contains-key? memory key)
      (give-with-hand hand (get-from-index memory key))))

  (remove-chunk [this key]
    (if (index-contains-key? memory key)
      (drop-with-hand hand key)))

  (forget-all [this]
    (clean-indexes memory))

  (rescan [this]
    ;;;existed storage processing
    (defn- load-existed-chunk-meta [hand position]
      (let [meta-buffer (create-buffer (:size-of-meta constants))]
        (.read hand (.clear meta-buffer) position)
        (buffer-to-meta meta-buffer)))

    (defn- load-existed-chunk-key [hand memory chunk-meta]
      (let [key (create-buffer (:size-of-key constants))
            position (+ (:position chunk-meta) (:size-of-meta constants))]
        (.read hand (.clear key) position)
        (if (= (:status chunk-meta) Byte/MAX_VALUE)
          (put-to-index memory key chunk-meta)
          (put-to-free memory key chunk-meta))
        chunk-meta))

    (locking hand
      (loop [position 0]
        ;(if (= (rem (count @index) 100) 0)
        ;  (println (count @index) "chunks were loaded."))
        (if (>= position (hand-size hand))
          true
          (recur
            (+ position
              (:cell-size
                (load-existed-chunk-key
                  (load-existed-chunk-meta position)))
              (:chunk-position-offset constants)))))))

  (compress [this] nil))

(defmacro defcarmen [name path-to-storage]
  `(defonce ~name (Carmen. (create-memory ) (create-hand ~path-to-storage))))