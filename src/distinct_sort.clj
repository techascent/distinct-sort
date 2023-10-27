(ns distinct-sort
  "Simple performance koan - given a large vector of maps or dataset what is the fastest way to
  get a sorted distinct set of one or more keys."
  (:require [net.cgrand.xforms :as xforms]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.resource :as resource]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf]
            [ham-fisted.protocols :as hamf-proto]
            [criterium.core :as crit]
            [tmducken.duckdb :as duckdb])
  (:import [java.util TreeSet Set List]
           [org.roaringbitmap RoaringBitmap]
           [tech.v3.datatype Buffer])
  (:gen-class))

(def data (hamf/vec (lznc/repeatedly 1000000 (fn [] {:name (str "some-name-" (rand-int 10000)) :supply (rand-int 50000)}))))

(defn load-dataset
  []
  (ds/->dataset data))


(def ds (load-dataset))


(defn xforms []
  (xforms/transjuxt [(comp (map :name) (distinct) (xforms/sort) (xforms/into []))
                     (comp (map :supply) (distinct) (xforms/sort) (xforms/into []))]
                    data))

(defn via-reduce-kv []
  (-> (reduce (fn [eax item]
                    (reduce-kv #(update %1 %2 conj %3) eax item))
                  {} data)
          (update-vals (comp vec sort set))))


(defn ds-set-sort []
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


(defn via-treeset []
  [(->> data (lznc/map :name) treeset)
   (->> data (lznc/map :supply) treeset)])


(defn pure-hamf-sort []
  [(->> data (lznc/map :name) (hamf/sort nil))
   (->> data (lznc/map :supply) (hamf/sort nil))])


(defn ds-hamf-hashset-sort []
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
  @(hamf-rf/preduce make-hashset-distinct hamf-rf/consumer-accumulator hamf-rf/reducible-merge data))

(defn parallel-distinct
  []
  [(->> ds :name hashset-distinct-parallel hamf/sort)
   (->> ds :supply hashset-distinct-parallel hamf/sort)])


(defn hamf-hashset-union
  []
  (let [rf (fn [data] (->> (hamf-rf/preduce hamf/mut-set #(do (.add ^Set %1 %2) %1) hamf/union data)
                           (hamf/sort nil)))]
    [(rf (ds :name))
     (rf (ds :supply))]))


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


(defn ds-concurrent-hashset-distinct
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

(defn map-singlepass-concurrent-hashset
  []
  (hamf-rf/preduce-reducers {:name (map-hashset-distinct-reducer :name)
                             :supply (map-hashset-distinct-reducer :supply)}
                            data))


(defn map-singlepass-hamf-union
  []
  (let [reducer-fn (fn [k]
                     (hamf-rf/parallel-reducer hamf/mut-set
                                               (fn [acc vm]
                                                 (.add ^Set acc (get vm k))
                                                 acc)
                                               hamf/union
                                               hamf/sort))]
    (hamf-rf/preduce-reducers {:name (reducer-fn :name)
                               :supply (reducer-fn :supply)}
                              data)))


(deftype ColHashsetDistinct [^Buffer col ^Set hs]
  java.util.function.LongConsumer
  (accept [this idx]
    (.add hs (.readObject col idx)))
  ham_fisted.Reducible
  (reduce [this rhs]
    (hamf/union hs (.-hs ^ColHashsetDistinct rhs))
    this)
  clojure.lang.IDeref
  (deref [this] (hamf/sort hs)))


(defn col-hashset-reducer
  [col]
  (let [col (dtype/->buffer col)]
    (hamf-rf/long-consumer-reducer #(ColHashsetDistinct. col (hamf/mut-set)))))


(defn ds-cols-singlepass
  []
  (hamf-rf/preduce-reducers {:name (col-hashset-reducer (ds :name))
                             :supply (col-hashset-reducer (ds :supply))}
                            (hamf/range (ds/row-count ds))))


(deftype ColCustomHashsetDistinct [^Buffer col ^Set hs]
  java.util.function.Consumer
  (accept [this rng]
    (let [sidx (long (rng 0))
          eidx (long (rng 1))]
      (.addAll hs (.subBuffer col (unchecked-int sidx) (unchecked-int eidx)))))
  ham_fisted.Reducible
  (reduce [this rhs]
    (hamf/union hs (.-hs ^ColCustomHashsetDistinct rhs))
    this)
  clojure.lang.IDeref
  (deref [this] (hamf/sort nil hs)))


(deftype BitmapCustomHashsetDistinct [^Buffer col ^RoaringBitmap hs]
  java.util.function.Consumer
  (accept [this rng]
    (let [sidx (long (rng 0))
          eidx (long (rng 1))]
      (.add hs (dtype/->int-array (.subBuffer col sidx eidx)))))
  ham_fisted.Reducible
  (reduce [this rhs]
    (.or hs (.-hs ^BitmapCustomHashsetDistinct rhs))
    this)
  clojure.lang.IDeref
  (deref [this] (bitmap/->random-access hs)))


(defn ds-cols-custom-singlepass
  []
  (let [rfn (fn [col]
              (hamf-rf/parallel-reducer
               (if (identical? :int32 (dtype/elemwise-datatype col))
                 #(BitmapCustomHashsetDistinct. (dtype/->buffer col) (bitmap/->bitmap))
                 #(ColCustomHashsetDistinct. (dtype/->buffer col) (hamf/mut-set)))
               hamf-rf/consumer-accumulator
               hamf-rf/reducible-merge
               deref))
        r (hamf-rf/compose-reducers {:name (rfn (ds :name))
                                     :supply (rfn (ds :supply))})
        rinit (hamf-proto/->init-val-fn r)
        rfn (hamf-proto/->rfn r)
        merge-fn (hamf-proto/->merge-fn r)
        n-rows (ds/row-count ds)]
    (hamf-proto/finalize
     r
     (->> (hamf/pgroups
           n-rows
           (fn [^long sidx ^long eidx]
             (rfn (rinit) [sidx eidx])))
          (reduce merge-fn)))))



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


(defmacro benchmark-ms
  "Benchmark an op, returning a map of :mean and :variance in ms."
  [f]
  `(let [bdata# (crit/quick-benchmark ~f nil)]
     {:mean-ms (* (double (first (:mean bdata#))) 1e3)
      :variance-ms (* (double (first (:variance bdata#))) 1e3)}))


(defn- bench-it
  [x]
  (let [nm (keyword (str (:name (meta x))))]
    (println "benchmarking" nm)
    (merge {:name nm}
           (benchmark-ms (x)))))


(defn run-full-benchmark
  []
  (println "running full benchmark ...")
  (let [profile-fns
        (lznc/concat
         [#'xforms
          #'load-dataset
          #'via-reduce-kv
          #'ds-set-sort
          #'hamf-lznc
          #'via-treeset
          #'pure-hamf-sort
          #'ds-hamf-hashset-sort
          #'parallel-distinct
          #'ds-concurrent-hashset-distinct
          #'map-singlepass-concurrent-hashset
          #'ds-cols-custom-singlepass]
         (when @duckdb-conn*
           [#'load-duckdb-data
            #'duckdb-prepared]))]
    (-> (ds/->dataset (map bench-it profile-fns))
        (ds/select-columns [:name :mean-ms :variance-ms])
        (ds/sort-by-column :mean-ms))))

(defn -main
  [& args]
  (println (run-full-benchmark)))

(comment



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
