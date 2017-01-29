(ns esource.help
  (:require [esource.core :refer [dispatch! state! on-event!]]
            [tools.fns :as fns]
            [tools.macros :refer [dochan]]))


(def by-name #(:name %2))


(defn event [type stream name data]
  {:name   name
   :data   data
   :date   (fns/now!)
   :stream stream
   :type   type})


(defn from-fn
  "For a given stream or stream and event(names) create a predicate to filter
  only maps containing such values in :type and :name keywords"
  ([stream]
   (fns/key-is? :type stream))

  ([stream event]
   (let [stream-is? (fns/key-is? :type stream)
         event-is?  (fns/key-is? :name event)]
     #(and (stream-is? %)
           (event-is? %)))))


(defn assert-event
  "For a given event object assert it has the required for processing.
  :name, :type, :stream, :data"
  [ev]
  (let [req-keys (juxt :name :type :stream :data)
        ev-keys  (filter some? (req-keys ev))]
    (assert (= (count ev-keys) 4)
            (str
             "Your event map does not have the required keys: "
             [:name :type :stream :data]))))


(defn event-consumer
  "For a given event name event on a stream, create a loop that calls f on new events of the
  same name"
  [stream event f]
  (let [ech (on-event! stream event)]
    (dochan [e ech] (f e))
    ech))
