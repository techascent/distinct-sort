(ns distinct-sort
  "Simple performance koan - given a large vector of maps or dataset what is the fastest way to
  get a sorted distinct set of one or more keys."
  (:require [net.cgrand.xforms :as xforms]
            [tech.v3.dataset :as dataset]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.resource :as resource]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf]
            [criterium.core :as crit]
            [tmducken.duckdb :as duckdb])
  (:import [java.util TreeSet]
           [org.roaringbitmap RoaringBitmap]
           [tech.v3.datatype Buffer]))

(def data (hamf/vec (lznc/repeatedly 1000000 (fn [] {:name (str "some-name-" (rand-int 10000)) :supply (rand-int 50000)}))))
(def ds (dataset/->dataset data))

(defn xforms []
  (xforms/transjuxt [(comp (map :name) (distinct) (xforms/sort) (xforms/into []))
                     (comp (map :supply) (distinct) (xforms/sort) (xforms/into []))]
                    data))

(defn reduce-kv []
  (-> (reduce (fn [eax item]
                    (reduce-kv #(update %1 %2 conj %3) eax item))
                  {} data)
          (update-vals (comp vec sort set))))


(defn dataset []
  [(-> ds :name set sort vec)
   (-> ds :supply set sort vec)])


(defn hamf-lznc []
  [(->> data (lznc/map :name) hamf/java-hashset hamf/sort)
   (->> data (lznc/map :supply) hamf/java-hashset hamf/sort)])


(defn treeset
  ([] (TreeSet.))
  ([data]
   (doto (TreeSet.)
     (.addAll data))))


(defn treeset []
  [(->> data (lznc/map :name) treeset)
   (->> data (lznc/map :supply) treeset)])


(defn hamf-sort []
  [(->> data (lznc/map :name) hamf/sort)
   (->> data (lznc/map :supply) hamf/sort)])


(defn hamf-dataset []
  [(->> ds :name hamf/java-hashset hamf/sort)
   (->> ds :supply hamf/java-hashset hamf/sort)])


(deftype HashsetDistinct [^java.util.Set hs ^java.util.List data]
  java.util.function.Consumer
  (accept [this val]
    (when (.add hs val)
      (.add data val)))
  ham_fisted.Reducible
  (reduce [this rhs]
    (reduce hamf-rf/consumer-accumulator this @rhs))
  clojure.lang.IDeref
  (deref [this] data))

(defn- make-hashset-distinct
  ([]
   (HashsetDistinct. (java.util.HashSet.) (hamf/object-array-list))))


(defn- hashset-distinct-parallel
  [data]
  ()
  @(hamf-rf/preduce make-hashset-distinct hamf-rf/consumer-accumulator hamf-rf/reducible-merge data))

(defn parallel-distinct
  []
  [(->> ds :name hashset-distinct-parallel hamf/sort)
   (->> ds :supply hashset-distinct-parallel hamf/sort)])


(deftype ConcurrentHashsetDistinct [^java.util.Set hs ^java.util.List data]
  java.util.function.Consumer
  (accept [this val]
    (when (.add hs val)
      (.add data val)))
  ham_fisted.Reducible
  (reduce [this rhs]
    (.addAll data @rhs)
    this)
  clojure.lang.IDeref
  (deref [this] data))


(defn make-concurrent-hashset-distinct
  [set]
  (ConcurrentHashsetDistinct. set (hamf/object-array-list)))


(defn concurrent-hashset
  []
  (java.util.concurrent.ConcurrentHashMap/newKeySet))

(defn- hashset-distinct-concurrent-parallel
  [data]
  (let [set (concurrent-hashset)]
    @(hamf-rf/preduce #(make-concurrent-hashset-distinct set)
                      hamf-rf/consumer-accumulator
                      hamf-rf/reducible-merge data)))


(defn concurrent-hashset
  []
  [(->> ds :name hashset-distinct-concurrent-parallel hamf/sort)
   (->> ds :supply hashset-distinct-concurrent-parallel hamf/sort)])


(deftype MapConcurrentHashsetDistinct [k ^java.util.Set hs ^java.util.List data]
  java.util.function.Consumer
  (accept [this val]
    (let [vv (get val k)]
      (when (.add hs vv)
        (.add data vv)))
    ham_fisted.Reducible)
  ham_fisted.Reducible
  (reduce [this rhs]
    (.addAll data (.-data ^MapConcurrentHashsetDistinct rhs))
    this)
  clojure.lang.IDeref
  (deref [this] (hamf/sort data)))

(defn map-hashset-distinct-reducer
  [k]
  (let [hs (concurrent-hashset)]
    (hamf-rf/consumer-preducer #(MapConcurrentHashsetDistinct. k hs (hamf/object-array-list)))))

(defn singlepass-concurrent-hashset
  []
  (hamf-rf/preduce-reducers {:name (map-hashset-distinct-reducer :name)
                             :supply (map-hashset-distinct-reducer :supply)}
                            data))

(defn ds-rows-singlepass-concurrent-hashset
  []
  (hamf-rf/preduce-reducers {:name (map-hashset-distinct-reducer :name)
                             :supply (map-hashset-distinct-reducer :supply)}
                            (dataset/rows ds)))


(deftype ColHashsetDistinct [^java.util.List col ^java.util.Set hs ^java.util.List data]
  java.util.function.LongConsumer
  (accept [this idx]
    (let [vv (.get col idx)]
      (when (.add hs vv)
        (.add data vv))))
  ham_fisted.Reducible
  (reduce [this rhs]
    (.addAll data (.-data ^ColHashsetDistinct rhs))
    this)
  clojure.lang.IDeref
  (deref [this] (hamf/sort data)))


(defn col-hashset-reducer
  [cname]
  (let [s (concurrent-hashset)
        col (dtype/->buffer (ds cname))]
    (hamf-rf/long-consumer-reducer #(ColHashsetDistinct. col s (hamf/object-array-list)))))


(defn ds-cols-singlepass
  []
  (hamf-rf/preduce-reducers {:name (col-hashset-reducer :name)
                             :supply (col-hashset-reducer :supply)}
                            {:rfn-datatype :int64}
                            (hamf/range (dataset/row-count ds))))


(deftype BitmapHashsetDistinct [^Buffer col ^RoaringBitmap hs]
  java.util.function.LongConsumer
  (accept [this idx]
    (let [vv (.readLong col idx)]
      (.add hs (unchecked-int vv))))
  ham_fisted.Reducible
  (reduce [this rhs]
    (.or hs (.-hs ^BitmapHashsetDistinct rhs))
    this)
  clojure.lang.IDeref
  (deref [this] (bitmap/->random-access hs)))


(defn col-typed-hashset-reducer
  [cname]
  (let [col (dtype/->buffer (ds cname))]
    (if (identical? :int32 (dtype/elemwise-datatype col))
      (hamf-rf/long-consumer-reducer #(BitmapHashsetDistinct. col (bitmap/->bitmap)))
      (col-hashset-reducer cname))))


(defn ds-cols-singlepass-typed
  []
  (hamf-rf/preduce-reducers {:name (col-typed-hashset-reducer :name)
                             :supply (col-typed-hashset-reducer :supply)}
                            {:rfn-datatype :int64}
                            (hamf/range (dataset/row-count ds))))



(defonce duckdb-db*  (delay (try
                              (do
                                (duckdb/initialize!)
                                (duckdb/open-db nil))
                              (catch Exception e
                                (println "Duckdb disabled - failed to load -" e)
                                nil))))

(defonce duckdb-conn* (delay (when @duckdb-db*
                               (duckdb/connect @duckdb-db*))))


(defn load-duckdb-data
  []
  (try (duckdb/drop-table! @duckdb-conn* "_unnamed")
       (catch Exception e))
  (duckdb/create-table! @duckdb-conn* ds)
  (duckdb/insert-dataset! @duckdb-conn* ds))


(defonce duckdb-table*
  (delay (when @duckdb-conn*
           (println "loading data into duckdb")
           (time
            (load-duckdb-data)))))

(defn duckdb
  []
  @duckdb-table*
  [(duckdb/sql->dataset @duckdb-conn* "select distinct name from _unnamed order by name")
   (duckdb/sql->dataset @duckdb-conn* "select distinct supply from _unnamed order by supply")])

(def prepared-statement*
  (delay [(duckdb/prepare @duckdb-conn* "select distinct name from _unnamed order by name" {:result-type :single})
          (duckdb/prepare @duckdb-conn* "select distinct supply from _unnamed order by supply" {:result-type :single})
          ]))


(defn duckdb-prepared
  []
  (mapv (fn [arg] (arg)) @prepared-statement*))

(comment

  (do
    (println "xforms")
    (crit/quick-bench (xforms)) ;; 770ms
    (println "reduce-kv")
    (crit/quick-bench (reduce-kv)) ;; 676ms
    (println "dataset")
    (crit/quick-bench (dataset)) ;; 385ms
    (println "hamf")
    (crit/quick-bench (hamf)) ;; 327ms
    (println "hamf-treeset")
    (crit/quick-bench (hamf-treeset)) ;; 327ms
    (println "hamf-sort")
    (crit/quick-bench (hamf-sort)) ;; 327ms
    (println "hamf-ds")
    (crit/quick-bench (hamf-ds)) ;; 211 ms
    (println "hamf-all-out")
    (crit/quick-bench (hamf-all-out)) ;;95ms
    (println "hamf-all-out-concurrent")
    (crit/quick-bench (hamf-all-out-concurrent)) ;;95ms
    (println "hamf-concurrent-singelpass")
    (crit/quick-bench (hamf-singlepass))
    (println "ds-singlepass")
    (crit/quick-bench (hamf-singlepass))
    (if @duckdb-conn*
      (do
        (println "duckdb")
        (crit/quick-bench (duckdb)) ;; 60ms
        (println "duckdb-prepared")
        (crit/quick-bench (duckdb-prepared)) ;; 32ms
        )
      (println "duckdb failed to load"))
    )

  ;;mac m1 timings
  ;;xforms - 181ms
  ;;reduce-kv - 181ms
  ;;dataset - 100ms
  ;;hamf - 77ms
  ;;hamf-ds - 50ms
  ;;hamf-all-out - 27ms
  ;;hamf-all-out-concurrent - 25ms
  ;;duckdb - load times eventually get down to 197ms, actual task duckdb - 13.3ms (!!)


  ;;desktop timings
  ;;xforms - 400ms
  ;;reduce-kv - 385ms
  ;;dataset - 157ms
  ;;hamf - 100ms
  ;;hamf-ds - 68ms
  ;;hamf-all-out 47ms
  ;;hamf-all-out-concurrent - 26ms
  ;;duckdb - 20ms
  )
