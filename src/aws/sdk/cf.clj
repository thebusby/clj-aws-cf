(ns aws.sdk.cf
  "Functions to access the Amazon Cloud Formation service.

  Each function takes a map of credentials as its first argument. The
  credentials map should contain an :access-key key and a :secret-key
  key."

  (:import com.amazonaws.AmazonServiceException
           com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.services.cloudformation.AmazonCloudFormationClient
           com.amazonaws.services.cloudformation.model.DescribeStacksResult
           com.amazonaws.services.cloudformation.model.Stack
           com.amazonaws.services.cloudformation.model.Output
           com.amazonaws.services.cloudformation.model.Parameter
           com.amazonaws.services.cloudformation.model.Tag
           com.amazonaws.services.cloudformation.model.DescribeStacksRequest
           com.amazonaws.services.cloudformation.model.GetTemplateRequest
           com.amazonaws.services.cloudformation.model.EstimateTemplateCostRequest
           com.amazonaws.services.cloudformation.model.CreateStackRequest
           com.amazonaws.services.cloudformation.model.DeleteStackRequest
           com.amazonaws.services.cloudformation.model.UpdateStackRequest
           )
  (:require [clojure.string :as string]))

(defn- cf-client*
  "Create an AmazonCloudFormationClient instance from a map of credentials."
  [cred]
  (if (:access-key cred)
    (AmazonCloudFormationClient.
     (BasicAWSCredentials.
      (:access-key cred)
      (:secret-key cred)))
    (AmazonCloudFormationClient.)))

(def ^{:private true}
  cf-client
  (memoize cf-client*))


;;
;; convert object graphs to clojure maps
;;

(defprotocol ^{:no-doc true} Mappable
  "Convert a value into a Clojure map."
  (^{:no-doc true} to-map [x] "Return a map of the value."))

(extend-protocol Mappable nil (to-map [_] nil))


;;
;; convert clojure maps to object graphs

(defn- keyword-to-method
  "Convert a dashed keyword to a CamelCase method name"
  [kw]
  (apply str (map string/capitalize (string/split (name kw) #"-"))))

(defn set-fields
  "Use a map of params to call setters on a Java object"
  [obj params]
  (doseq [[k v] params]
    (let [method-name (str "set" (keyword-to-method k))
          method (first (clojure.lang.Reflector/getMethods (.getClass obj) 1 method-name false))
          arg-type (first (.getParameterTypes method))
          arg (if (= arg-type java.lang.Integer) (Integer. v) v)]
      (clojure.lang.Reflector/invokeInstanceMember method-name obj arg)))
  obj)

(declare mapper)

(defn map->ObjectGraph
  "Transform the map of params to a graph of AWS SDK objects"
  [params]
  (let [keys (keys params)]
    (zipmap keys (map #((mapper %) (params %)) keys))))

(defmacro mapper->
  "Creates a function that invokes set-fields on a new object of type
   with mapped parameters."
  [type]
  `(fn [~'params] (set-fields (new ~type) (map->ObjectGraph ~'params))))

(defn- mapper
  ""
  [key]
  (let [mappers {:parameters (fn [params] (map (fn [[k v]] (doto (Parameter.)
                                                             (.setParameterKey (name k))
                                                             (.setParameterValue v)))
                                           params))}]
    (if (contains? mappers key) 
      (mappers key) 
      identity)))


;;
;; exceptions
;;

(extend-protocol Mappable
  AmazonServiceException
  (to-map [e]
    {:error-code   (.getErrorCode e)
     :error-type   (.name (.getErrorType e))
     :service-name (.getServiceName e)
     :status-code  (.getStatusCode e)}))

(defn decode-exceptions
  "Returns a Clojure map containing the details of an AmazonServiceException"
  [& exceptions]
  (map to-map exceptions))


;;
;; zones
;; 

(extend-protocol Mappable
  DescribeStacksResult
  (to-map [dsr]
    {:next-token (.getNextToken dsr)
     :stacks (map to-map (.getStacks dsr))})
  Stack
  (to-map [s]
    {:capabilities (vec (.getCapabilities s))
     :creation-time (.getCreationTime s)
     :description (.getDescription s)
     :disable-rollback (.getDisableRollback s)
     :last-updated-time (.getLastUpdatedTime s)
     :notification-arns (vec (.getNotificationARNs s))
     :outputs (map to-map (.getOutputs s))
     :parameters (map to-map (.getParameters s))
     :stack-id (.getStackId s)
     :stack-name (.getStackName s)
     :stack-status (.getStackStatus s)
     :stack-status-reason (.getStackStatusReason s)
     :tags (map to-map (.getTags s))
     :timeout-in-minutes (.getTimeoutInMinutes s)
     })  
  Output
  (to-map [o]
    {:description (.getDescription o)
     :output-key (.getOutputKey o)
     :output-value (.getOutputValue o)})
  Parameter
  (to-map [p]
    {:parameter-key (.getParameterKey p)
     :parameter-value (.getParameterValue p)})
  Tag
  (to-map [t]
    {:key (.getKey t)
     :value (.getValue t)})
  )

(defn describe-stacks
  "List stack or all stacks"
  ([cred] (map to-map 
               (.getStacks (.describeStacks (cf-client cred)))))
  ([cred stack-name] (map to-map 
                          (.getStacks (.describeStacks (cf-client cred)
                                                       (doto (DescribeStacksRequest.)
                                                         (.setStackName stack-name)))))))

(defn get-template
  "Return template for named stack"
  [cred stack-name]
  (.getTemplateBody (.getTemplate (cf-client cred)
                                  (doto (GetTemplateRequest.)
                                    (.setStackName stack-name)))))

(defn estimate-cost-url
  "Return an URL for an estimate of the template's monthly cost."
  [cred template]
  (.getUrl (.estimateTemplateCost (cf-client cred)
                                  (doto (EstimateTemplateCostRequest.)
                                    (.setTemplateBody template)))))

(defn create-stack
  "Create a new stack, and return the new Stack ID."
  [cred params]
  (.getStackId (.createStack (cf-client cred)
                             ((mapper-> CreateStackRequest) params))))

(defn update-stack
  "Update an existing stack, and return the Stack ID."
  [cred params]
  (.getStackId (.updateStack (cf-client cred)
                             ((mapper-> UpdateStackRequest) params))))

(defn delete-stack
  "Delete an existing stack by name."
  [cred stack-name]
  (.deleteStack (cf-client cred)
                (doto (DeleteStackRequest.)
                  (.setStackName stack-name))))




(comment 

  ;; A few examples...
  ;;
  (def my-template (slurp "/home/abusby/github/clj-aws-cf/test.template"))

  (create-stack cred {:stack-name "cf-test-stack"
                      :template-body my-template
                      :parameters {"KeyName" "GNACRTEST"}
                      })

  (create-stack cred {:stack-name "cf-test-stack"
                      :template-body (slurp "/home/abusby/github/clj-aws-cf/test2.template")
                      :parameters {"ImageId" "ami-db3479b2" 
                                   "dbname"  "foo-db-1.0.2" 
                                   "elbname" "elb-cf-test"}
                      })

)


