(ns onyx.plugin.benchmark-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [onyx.plugin.elasticsearch :as e]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.seq]
            [onyx.job :refer [add-task]]
            [onyx.tasks.core-async]
            [onyx.tasks.seq]
            [onyx.tasks.null]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [onyx.tasks.function]
            [onyx.tasks.elasticsearch]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.test-helper :refer [with-test-env]]
            [qbits.spandex :as spdx]))

(def id (java.util.UUID/randomUUID))

(def zk-addr "127.0.0.1:2188")

(def env-config
  {:onyx/tenancy-id id
   :zookeeper/address zk-addr
   :zookeeper/server? true
   :zookeeper.server/port 2188})

(def peer-config
  {:onyx/tenancy-id id
   :zookeeper/address zk-addr
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging.aeron/embedded-driver? true
   :onyx.messaging/allow-short-circuit? false
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(def n-messages 7)

(def batch-size 20)

(defn build-job [workflow compact-job task-scheduler]
  (reduce (fn [job {:keys [name task-opts type args] :as task}]
            (case type
              :seq (add-task job (apply onyx.tasks.seq/input-serialized name task-opts (:input task) args))
              :elastic (add-task job (apply onyx.tasks.elasticsearch/output name task-opts args))
              :fn (add-task job (apply onyx.tasks.function/function name task-opts args))))
          {:workflow workflow
           :catalog []
           :lifecycles []
           :triggers []
           :windows []
           :task-scheduler task-scheduler}
          compact-job))




(defn run-test-job [job n-peers]
  (let [id (random-uuid)
        env-config env-config
        peer-config peer-config]
    (with-test-env [test-env [n-peers env-config peer-config]]

                   (let [{:keys [job-id]} (onyx.api/submit-job peer-config job)
                         _ (assert job-id "Job was not successfully submitted")
                         _ (onyx.test-helper/feedback-exception! peer-config job-id)
                         out-channels (onyx.plugin.core-async/get-core-async-channels job)]
                     (into {}
                           (map (fn [[k chan]]
                                  [k (take-segments! chan 50)])
                                out-channels))))))

(defn get-document-rest-request
  [doc-id name]
  {:elasticsearch/index :get_together
   :elasticsearch/mapping-type :group
   :elasticsearch/write-type :index
   :elasticsearch/id doc-id
   :elasticsearch/message {:name name}})

(def es-host "http://127.0.0.1:9200")

(def test-index :output_test)

(def write-elastic-opts
  {:elasticsearch/host es-host})

(def write-elastic-opts-default
  {:elasticsearch/host es-host
   :elasticsearch/index test-index})

(def default-test
  [{:elasticsearch/message {:name "http:insert_detail-msg_noindex" :index "default_one"}
    :elasticsearch/mapping-type :group
    :elasticsearch/write-type :index}])

(def data-file (io/resource
                "documents.json"))
(defn get-documents []
  (let [docs (json/read-str (slurp data-file))]

    (clojure.pprint/pprint docs)
    docs
    ))

(defn- index-documents
  ([] (index-documents false))
  ([bulk?] (let [documents (get-documents)
                 input (map #(merge {:elasticsearch/message %
                                     :elasticsearch/id (.toString (java.util.UUID/randomUUID))}
                                    {:elasticsearch/index test-index
                                     :elasticsearch/mapping-type :group
                                     :elasticsearch/write-type :index}) documents)
                 task-opts {:onyx/batch-size 20
                            :elasticsearch/bulk true}
                 job-write (build-job [[:in :write-elastic]]
                                      [{:name :in
                                        :type :seq
                                        :task-opts task-opts
                                        :input input}
                                       {:name :write-elastic
                                        :type :elastic
                                        :task-opts (merge task-opts write-elastic-opts)}]
                                      :onyx.task-scheduler/balanced)]
             (run-test-job job-write 3)
             ;; Wait for Elasticsearch to update
             (Thread/sleep 7000))))

(defn- index-documents-bulk [] (index-documents true))

(defn delete-index [index]
  (let [client (spdx/client {:hosts [es-host]})]
    (try
      (spdx/request client {:url [index] :method :delete})
      (catch Exception e (prn (str "caught exception: " (.getMessage e)))))))

(defn create-index [index]
  (let [client (spdx/client {:hosts [es-host]})]
    (spdx/request client {:url [index] :method :put})))

(defn- search [client body]
  (spdx/request client {:url [test-index :group :_search] :method :get :body body}))

;; tests are ran twice, first in a non-bulk context, then with bulk enabled
(use-fixtures :once (fn [f]
                      (delete-index test-index)
                      (create-index test-index)
                      (f)))

(deftest check-http&write-job
  (testing "Insert large documents"
    (index-documents-bulk)))
