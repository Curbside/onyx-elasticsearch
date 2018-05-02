(ns onyx.plugin.spandex-elasticsearch
  (:require [onyx.plugin.protocols :as p]
            [onyx.static.default-vals :refer [default-vals arg-or-default]]
            [taoensso.timbre :as log]
            [qbits.spandex :as sp]
            [schema.core :as s]))

(defn- rest-method
  [write-type id]
  (case write-type
    :index  (if (some? id) :put :post)
    :update :post
    :upsert :post
    :update-by-query :post
    :delete :delete
    (throw (Exception. (str "Invalid write type: " write-type)))))

(def base-write-request {:elasticsearch/index s/Keyword
                         :elasticsearch/mapping-type s/Keyword
                         :elasticsearch/write-type s/Keyword})

(def index-request (merge base-write-request {(s/optional-key :elasticsearch/id) s/Any
                                               :elasticsearch/message {s/Keyword s/Any}}))

(def update-request (merge base-write-request {:elasticsearch/id s/Any
                                               :elasticsearch/message {s/Keyword s/Any}}))

(def update-by-query-request (merge base-write-request {:elasticsearch/message {:query {s/Keyword s/Any} :script {s/Keyword s/Any}}}))

(def delete-request (merge base-write-request {:elasticsearch/id s/Any}))

(defn- validation-schema
  [{write-type :elasticsearch/write-type}]
  (case write-type
    :index  index-request
    :update update-request
    :upsert update-request
    :update-by-query update-by-query-request
    :delete delete-request
    (throw (Exception. (str "Invalid write type: " write-type)))))

(defn- wrap-update [write-type message]
  (let [doc {:doc message}]
    (if (= write-type :upsert) 
      (assoc doc :doc_as_upsert true)
      doc)))

(s/defn rest-request
  "Takes in a settings map and returns a REST request to send to the spandex client."
  [options]
  (s/validate (validation-schema options) options)
  (let [{:keys [:elasticsearch/index
                :elasticsearch/mapping-type
                :elasticsearch/write-type
                :elasticsearch/id
                :elasticsearch/message]} options]
    {:url (cond-> [index mapping-type] (some? id) (conj id) (contains? #{:update :upsert} write-type) (conj :_update))
     :method (rest-method write-type id)
     :body (let [doc (or message {})] 
             (if (contains? #{:update :upsert} write-type)
               (wrap-update write-type doc)
               doc))}))

(defrecord ElasticSearchReader []
  p/Plugin
  (start [this event] this)
  (stop [this event] this)
  
  p/Checkpointed
  (recover! [this replica-version checkpoint])
  (checkpoint [this])
  (checkpointed! [this epoch])

  p/BarrierSynchronization
  (synced? [this epoch]
    true)
  (completed? [this] true)

  p/Input
  (poll! [this segment _]
    ))

(defn- merge-with-defaults
  [event doc-defaults]
  (if (or (= :delete (:elasticsearch/write-type event)) (contains? event :elasticsearch/message))
    (merge doc-defaults (select-keys 
                         event [:elasticsearch/index
                                :elasticsearch/id
                                :elasticsearch/mapping-type
                                :elasticsearch/write-type
                                :elasticsearch/message]))
    (assoc doc-defaults :elasticsearch/message event)))

(defrecord ElasticSearchWriter []
  p/Plugin
  (start [this event] this) 
  (stop [this event] this)

  p/Checkpointed
  (recover! [this replica-version checkpoint])
  (checkpoint [this])
  (checkpointed! [this epoch])

  p/BarrierSynchronization
  (synced? [this epoch]
    true)
  (completed? [this] true)

  p/Output
  (prepare-batch [this event replica messenger] 
    true)
  (write-batch 
    [this {:keys [onyx.core/write-batch elasticsearch/connection elasticsearch/doc-defaults]} replica messenger]
    (doseq [event write-batch]
      (println event)
      (sp/request connection (rest-request (merge-with-defaults event doc-defaults))))
    true))

(defn output [event]
  (->ElasticSearchWriter))
