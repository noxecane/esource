(ns esource.help
  (:require [esource.core :refer [dispatch! state! on-event!]]
            [monger.core :as monger]
            [monger.query :as query]
            [contrib.core :as contrib :refer [dochan]]))


(defn event [type stream name data]
  {:name   name
   :data   data
   :date   (contrib/now!)
   :stream stream
   :type   type})


(defn from-fn
  "For a given stream or stream and event(names) create a predicate to filter
  only maps containing such values in :type and :name keywords"
  ([stream]
   (contrib/key-is? :type stream))

  ([stream event]
   (let [stream-is? (contrib/key-is? :type stream)
         event-is?  (contrib/key-is? :name event)]
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


(defmacro doevent
  "For a given event name on a stream, create a subscription and a consumer over
  each event e."
  [stream event & body]
  (let [event-var 'e]
    `(let [ech# (on-event! ~stream ~event)]
       (dochan [~event-var ech#]
         ~@body))))


(defn max-document
  ([db coll docquery field]
   (first (query/with-collection db coll
            (query/find docquery)
            (query/sort (array-map field -1))
            (query/limit 1))))

  ([db coll docquery fields field]
   (first (query/with-collection db coll
            (query/find docquery)
            (query/fields fields)
            (query/sort (array-map field -1))
            (query/limit 1)))))


(defmacro defreducer [name]
  `(defmulti ~name #(:name %2)))
