(ns carmen.core
  (:use [carmen.tools]
        [carmen.hands]
        [carmen.index]))

;;TODO add descriptions and README

;;storage operations

;;TODO: add logger
;;TODO: improve finding of chunk cell for overwriting and make it configurable
;;TODO: compressor function
;;TODO: add checksums
;;TODO: ciphering
;;TODO: large keys?
;;TODO: exceptions processing
;;TODO: drain function
;;TODO: web server

;;business protocols

(defprotocol PStore
  (persist-chunk [this key chunk-body])
  (get-chunk [this key])
  (remove-chunk [this key])
  (forget-all [this])
  (rescan [this])
  (compress [this])
  (used-space [this]))

(deftype Store [^carmen.index.PHandMemory memory
                ^carmen.hands.PHand hand]
  PStore
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
      (give-with-hand hand
        (get-from-index memory key))))

  (remove-chunk [this key]
    (if (index-contains-key? memory key)
      (do
        (drop-with-hand hand (get-from-index memory key))
        (move-from-index-to-free memory key))))

  (forget-all [this]
    (clean-indexes memory))

  ;;TODO: refactor this
  (rescan [this]
    (defn- load-existed-chunk-key [chunk-meta]
      (let [key (read-key hand chunk-meta)]
        (if (= (:status chunk-meta) Byte/MAX_VALUE)
          (put-to-index memory key chunk-meta)
          (put-to-free memory key chunk-meta))
        chunk-meta))

    (locking hand
      (loop [position 0
             chunks-were-loaded 0]
        (if (= (rem chunks-were-loaded 100) 0)
          (println chunks-were-loaded "chunks were loaded."))
        (if (>= position (hand-size hand))
          chunks-were-loaded
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

(deftype StoreProxy [proxy-list consistency]
  PStore
  (persist-chunk [this key chunk-body]
    (let [lazy-result (map #(persist-chunk % key chunk-body) proxy-list)]
      (case consistency
        :one (drop-while false? lazy-result)
        :quorum ()
        :all ())))

  (get-chunk [this key] nil)
  (remove-chunk [this key] nil)
  (forget-all [this] nil)
  (rescan [this] nil)
  (compress [this] nil)
  (used-space [this] nil))
