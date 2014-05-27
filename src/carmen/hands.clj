(ns carmen.hands
  (:use [carmen.tools]))

;TODO: try to FChannel lock hand file
;TODO: add checksum to every cell

(defprotocol Handly
  (take-in-hand [this key chunk-body])
  (retake-in-hand [this key chunk-body chunk-meta])
  (give-with-hand [this chunk-meta])
  (drop-with-hand [this chunk-meta])
  (wash-hand [this])
  (compact-hand [this])
  (hand-size [this]))

(deftype Hand [channel]
  Handly
  (take-in-hand [this key chunk-body]
    (locking channel
      (let [position (.size channel)
            cell-size (+ (:chunk-position-offset constants) (.capacity chunk-body))
            chunk-meta {:status Byte/MAX_VALUE
                        :position position
                        :size (.capacity chunk-body)
                        :cell-size cell-size}
            buffer (wrap-key-chunk-and-meta key chunk-body chunk-meta)]
        (while (.hasRemaining buffer)
          (.write channel buffer position))
        chunk-meta)))

  (retake-in-hand [this key chunk-body chunk-meta]
    (let [position (:position chunk-meta)
          new-chunk-meta {:status Byte/MAX_VALUE
                          :position position
                          :size (.capacity chunk-body)
                          :cell-size (:cell-size chunk-meta)}
          buffer (wrap-key-chunk-and-meta key chunk-body new-chunk-meta)]
      (locking channel
        (while (.hasRemaining buffer)
          (.write channel buffer position)))
      new-chunk-meta))

  (give-with-hand [this chunk-meta]
    (let [chunk-body (.clear (create-buffer (:size chunk-meta)))
          chunk-position (+ (:position chunk-meta) (:chunk-position-offset constants))]
      (locking channel
        (while (.hasRemaining chunk-body)
          (.read channel chunk-body chunk-position)))
      (.rewind chunk-body)))

  (drop-with-hand [this chunk-meta]
    (let [position (:position chunk-meta)
          buffer (.rewind (.put (create-buffer 1) 0 Byte/MIN_VALUE))]
      (locking channel
        (while (.hasRemaining buffer)
          (.write channel buffer position))))
    (assoc chunk-meta :status Byte/MIN_VALUE))

  (wash-hand [this]
    (locking channel
      (.truncate channel 0)))

  (compact-hand [this]
    (locking channel
      nil))

  (hand-size [this]
    (locking channel
      (.size channel))))

(defmacro defhand [handname filepath]
  `(defonce ~handname (Hand. (get-channel-of-file ~filepath))))