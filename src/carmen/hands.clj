(ns carmen.hands
  (:require [taoensso.timbre :as timbre])
  (:use [carmen.tools]))

(defprotocol PHand
  (take-in-hand [this key chunk-body ttl] [this key chunk-body])
  (retake-in-hand [this key chunk-body ttl chunk-meta] [this key chunk-body chunk-meta])
  (give-with-hand [this chunk-meta])
  (drop-with-hand [this chunk-meta])
  (wash-hand [this])
  (compact-hand [this])
  (hand-size [this])
  (read-meta [this position])
  (read-key [this chunk-meta])
  (holistic? [this chunk-meta]))

(deftype Hand [channel lock]
  PHand
  (take-in-hand [this key chunk-body ttl]
    (locking channel
      (let [position (.size channel)
            cell-size (+ (:not-chunk-size constants) (.capacity chunk-body))
            chunk-meta {:status Byte/MAX_VALUE
                        :position position
                        :size (.capacity chunk-body)
                        :cell-size cell-size
                        :born (System/currentTimeMillis )
                        :ttl ttl}
            buffer (wrap-key-chunk-and-meta key chunk-body chunk-meta)]
        (while (.hasRemaining buffer)
          (.write channel buffer position))
        (.force channel true)
        chunk-meta)))

  (take-in-hand [this key chunk-body]
    (take-in-hand this key chunk-body 0))

  (retake-in-hand [this key chunk-body ttl chunk-meta]
    (let [position (:position chunk-meta)
          new-chunk-meta {:status Byte/MAX_VALUE
                          :position position
                          :size (.capacity chunk-body)
                          :cell-size (:cell-size chunk-meta)
                          :born (System/currentTimeMillis )
                          :ttl ttl}
          buffer (wrap-key-chunk-and-meta key chunk-body new-chunk-meta)]
      (locking channel
        (while (.hasRemaining buffer)
          (.write channel buffer position))
        (.force channel true))
      new-chunk-meta))

  (retake-in-hand [this key chunk-body chunk-meta]
    (retake-in-hand this key chunk-body 0 chunk-meta))

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
          (.write channel buffer position))
        (.force channel true)))
    (assoc chunk-meta :status Byte/MIN_VALUE))

  (wash-hand [this]
    (locking channel
      (.truncate channel 0)))

  (compact-hand [this]
    (locking channel
      nil))

  (hand-size [this]
    (locking channel
      (.size channel)))

  ;;;be careful to directly use next three methods!

  (read-meta [this position]
    (let [meta-buffer (create-buffer (:size-of-meta constants))]
      (.read channel (.clear meta-buffer) position)
      (buffer-to-meta meta-buffer)))

  (read-key [this chunk-meta]
    (let [key (.clear (create-buffer (:size-of-key constants)))
          position (+ (:position chunk-meta) (:size-of-meta constants))]
      (while (.hasRemaining key)
        (.read channel key position))
      key))

  (holistic? [this chunk-meta]
    (let [key (read-key this chunk-meta)
          chunk (give-with-hand this chunk-meta)
          chunk-meta-buffer (wrap-chunk-meta chunk-meta)
          expected-checksum (hash-buffers key chunk chunk-meta-buffer)
          checksum-buffer (.clear (create-buffer (:size-of-checksum constants)))
          checksum-position (+ (:position chunk-meta)
                              (:chunk-position-offset constants)
                              (:size chunk-meta))]
      (while (.hasRemaining checksum-buffer)
        (.read channel checksum-buffer checksum-position))
      (= (.getInt checksum-buffer 0) expected-checksum))))

(defn create-hand [filepath]
  (let [channel (get-channel-of-file filepath)]
    (Hand. channel (.lock channel))))