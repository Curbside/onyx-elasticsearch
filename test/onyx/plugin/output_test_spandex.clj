(ns onyx.plugin.output-test-spandex
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [onyx.plugin.spandex-elasticsearch :as e]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.seq]
            [onyx.job :refer [add-task]]
            [onyx.tasks.core-async]
            [onyx.tasks.seq]
            [onyx.tasks.null]
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

(def test-set
  [{:elasticsearch/message {:name "http:insert_detail-msg_noid" :index "one"}
    :elasticsearch/index test-index
    :elasticsearch/mapping-type :group
    :elasticsearch/write-type :index}
   {:elasticsearch/message {:name "http:insert_detail-msg_id"}
    :elasticsearch/index test-index
    :elasticsearch/mapping-type :group
    :elasticsearch/write-type :index
    :elasticsearch/id "1"}
   {:elasticsearch/message {:name "http:insert_detail-msg_id" :new "new"}
    :elasticsearch/index test-index
    :elasticsearch/mapping-type :group
    :elasticsearch/write-type :update
    :elasticsearch/id "1"}
   {:elasticsearch/message {:name "http:insert_detail-msg_id"}
    :elasticsearch/index test-index
    :elasticsearch/mapping-type :group
    :elasticsearch/write-type :upsert
    :elasticsearch/id "2"}
   {:elasticsearch/message {:name "http:insert-to-be-deleted"}
    :elasticsearch/index test-index
    :elasticsearch/mapping-type :group
    :elasticsearch/write-type :index
    :elasticsearch/id "3"}
   {:elasticsearch/index test-index
    :elasticsearch/mapping-type :group
    :elasticsearch/write-type :delete
    :elasticsearch/id "3"}])

(def default-test
  [{:elasticsearch/message {:name "http:insert_detail-msg_noindex" :index "default_one"}
    :elasticsearch/mapping-type :group
    :elasticsearch/write-type :index}])

(defn index-documents []
  (let [n-messages 5
        task-opts {:onyx/batch-size 20}
        job-write (build-job [[:in :write-elastic]]
                                        [{:name :in
                                          :type :seq 
                                          :task-opts task-opts 
                                          :input test-set} 
                                         {:name :write-elastic
                                          :type :elastic
                                          :task-opts (merge task-opts write-elastic-opts)}]
                        :onyx.task-scheduler/balanced)
        job-write-default (build-job [[:in :write-elastic]]
                               [{:name :in
                                 :type :seq 
                                 :task-opts task-opts
                                 :input default-test} 
                                {:name :write-elastic
                                 :type :elastic
                                 :task-opts (merge task-opts write-elastic-opts-default)}]
                               :onyx.task-scheduler/balanced)]
    (run-test-job job-write 3)
    (run-test-job job-write-default 3)
    ; Wait for Elasticsearch to update
    (Thread/sleep 7000)))

(defn delete-index [index]
  (let [client (spdx/client {:hosts [es-host]})]
    (spdx/request client {:url [index] :method :delete})))

(defn- search [client body]
  (spdx/request client {:url [test-index :group :_search] :method :get :body body}))

(use-fixtures :once (fn [f]
                      (index-documents)
                      (f)
                      (delete-index test-index)))

(let [client (spdx/client {:hosts [es-host]})]

  (deftest check-http&write-job
    (testing "Insert: plain message with no id defined"
      (let [{:keys [:body]} (search client {:query {:match {:index "one"}}})]
        (is (= 1 (get-in body [:hits :total])))
        (is (not-empty (first (get-in body [:hits :hits]))))))
    (let [{:keys [:body]} (search client {:query {:match {:_id "1"}}})]
        (testing "Insert: detail message with id defined"
          (is (= 1 (get-in body [:hits :total]))))
        (testing "Update: detail message with id defined"
          (is (= "new" (get-in (first (get-in body [:hits :hits])) [:_source :new])))))
    (testing "Upsert: detail message with id defined"
      (let [{:keys [:body]} (search client {:query {:match {:_id "2"}}})]
        (is (= 1 (get-in body [:hits :total])))))
    (testing "Delete: detail defined"
      (let [{:keys [:body]} (search client {:query {:match {:_id "3"}}})]
        (is (= 0 (get-in body [:hits :total]))))))

  (deftest check-write-defaults
    (testing "Insert: plain message with no index in segment but defined in opts"
      (let [{:keys [:body]} (search client {:query {:match {:index "default_one"}}})]
        (is (= 1 (get-in body [:hits :total])))
        (is (not-empty (first (get-in body [:hits :hits]))))))))
