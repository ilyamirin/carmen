(ns carmen.hands
  (:require [taoensso.timbre :as timbre])
  (:use [carmen.tools]))

;TODO: try to FChannel lock hand file

(defprotocol PHand
  (take-in-hand [this key chunk-body ttl] [this key chunk-body])
  (retake-in-hand [this key chunk-body ttl chunk-meta] [this key chunk-body chunk-meta])
  (give-with-hand [this chunk-meta])
  (drop-with-hand [this chunk-meta])
  (wash-hand [this])
  (compact-hand [this])
  (hand-size [this])
  (read-meta [this position])
  (read-key [this chunk-meta]))

(deftype Hand [channel]

  PHand

  (take-in-hand [this key chunk-body ttl]
    (locking channel
      (let [position (.size channel)
            cell-size (+ (:chunk-position-offset constants) (.capacity chunk-body))
            chunk-meta {:status Byte/MAX_VALUE
                        :position position
                        :size (.capacity chunk-body)
                        :cell-size cell-size
                        :born (System/currentTimeMillis )
                        :ttl ttl}
            buffer (wrap-key-chunk-and-meta key chunk-body chunk-meta)]
        (while (.hasRemaining buffer)
          (.write channel buffer position))
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
          (.write channel buffer position)))
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
      (.size channel)))

  ;;;be careful to directly use next two methods!
  ;;;TODO: test next two methods

  (read-meta [this position]
    (let [meta-buffer (create-buffer (:size-of-meta constants))]
      (.read channel (.clear meta-buffer) position)
      (buffer-to-meta meta-buffer)))

  (read-key [this chunk-meta]
    (let [key (create-buffer (:size-of-key constants))
          position (+ (:position chunk-meta) (:size-of-meta constants))]
      (.read channel (.clear key) position)
      key)))

(defn create-hand [filepath]
  (Hand. (get-channel-of-file filepath)))