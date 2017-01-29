(ns esource.inproc
  (:require [clojure.core.async :as a]
            [esource.core :refer [IBus] :as esource]
            [tools.fns :as fns]))


(defrecord InProcessBus [in sock]
  IBus

  (publish! [_ event]
    (a/put! in event))

  (subscribe! [bus mine?]
    (esource/subscribe! bus mine? 10))

  (subscribe! [bus mine? buff]
    (let [out (a/chan buff (filter mine?))]
      (a/tap sock out)
      out))

  (unsubscribe! [bus chan]
    (a/untap sock chan)
    (a/close! chan))

  (stop! [_]
    (a/close! in)))


(defn new-bus
  "For a given buffer size buff, create an InProcessBus. The default buffer size
  is 100"
  ([]
   (new-bus 100))

  ([buff]
   (let [in   (a/chan buff)
         sock (a/mult in)]
     (->InProcessBus in sock))))
