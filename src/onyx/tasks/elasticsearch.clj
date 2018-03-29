(ns onyx.tasks.elasticsearch
  (:require [schema.core :as s]
            [taoensso.timbre :as log]
            [qbits.spandex :as sp]))

(defn inject-writer
  [{{host :elasticsearch/host
     index :elasticsearch/index
     id :elasticsearch/id
     mapping-type :elasticsearch/mapping-type
     write-type :elasticsearch/write-type
     :or {mapping-type :_default_
          write-type :index}} :onyx.core/task-map} _]
  (log/info (str "Creating ElasticSearch http client for " host))
  {:elasticsearch/connection (sp/client {:hosts [host]})
   :elasticsearch/doc-defaults (merge {:elasticsearch/mapping-type mapping-type
                                       :elasticsearch/write-type write-type}
                                      (when index {:elasticsearch/index index})
                                      (when id {:elasticsearch/id id}))})

(def writer-lifecycles
  {:lifecycle/before-task-start inject-writer})

(s/defn output
  [task-name :- s/Keyword opts]
  (println opts)
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/plugin :onyx.plugin.spandex-elasticsearch/output
                            :onyx/type :output
                            :onyx/medium :elasticsearch
                            :onyx/max-peers 1
                            :onyx/doc "Writes segments to an Elasticsearch cluster."}
                           opts)
          :lifecycles [{:lifecycle/task task-name
                        :lifecycle/calls ::writer-lifecycles}]}})
