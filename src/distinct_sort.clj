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
            [ham-fisted.set :as hamf-set]
            [criterium.core :as crit]
            [tmducken.duckdb :as duckdb])
  (:import [java.util TreeSet Set List]
           [org.roaringbitmap RoaringBitmap]
           [tech.v3.datatype Buffer])
  (:gen-class))

;;hamf vector as the parallelization pathway faster.
(defn make-dataset
  []
  (->> (lznc/repeatedly 1000000 (fn [] {:name (str "some-name-" (rand-int 10000))
                                        :supply (rand-int 50000)}))
       (hamf/vec)))

(def vec-data (make-dataset))

(defn load-dataset
  []
  (ds/->dataset vec-data))


(defn parallel-load-dataset
  []
  (->> (hamf/pgroups
        (count vec-data)
        (fn [^long sidx ^long eidx]
          (ds/->dataset (hamf/subvec vec-data sidx eidx))))
       (apply ds/concat)))


(def ds (parallel-load-dataset))

;; distinct-sort> schema
;; [{:categorical? true, :name :name, :datatype :string, :n-elems 1000000}
;;  {:name :supply, :datatype :int32, :n-elems 1000000}]
(def schema (mapv meta (vals ds)))


(defn xforms []
  (xforms/transjuxt [(comp (map :name) (distinct) (xforms/sort) (xforms/into []))
                     (comp (map :supply) (distinct) (xforms/sort) (xforms/into []))]
                    vec-data))

(defn via-reduce-kv []
  (-> (reduce (fn [eax item]
                    (reduce-kv #(update %1 %2 conj %3) eax item))
                  {} vec-data)
          (update-vals (comp vec sort set))))


(defn ds-set-sort []
  [(-> ds :name set sort vec)
   (-> ds :supply set sort vec)])

(defn hamf-sort
  [data]
  ;;Passing in nil for the comparator tells hamf to explicitly avoid
  ;;using the default clojure comparator.  This makes the sort more fragile
  ;;but a bit faster specifically in the case of a stream of integers.
  (hamf/sort nil data))


(defn hamf-lznc []
  [(->> vec-data (lznc/map :name) hamf/java-hashset hamf-sort)
   (->> vec-data (lznc/map :supply) hamf/java-hashset hamf-sort)])


(defn treeset
  ([] (TreeSet.))
  ([vec-data]
   (doto (TreeSet.)
     (.addAll vec-data))))


(defn via-treeset []
  [(->> vec-data (lznc/map :name) treeset)
   (->> vec-data (lznc/map :supply) treeset)])


(defn inorder-dedup
  [data]
  (mapv #(reduce (fn [acc v] v) nil %) (lznc/partition-by identity data)))


(defn via-hamf-sort
  "Out of curiosity, how does a pure sort of the data followed by an in-order deduplication compare?"
  []
  [(->> vec-data (lznc/map :name) hamf-sort inorder-dedup)
   (->> vec-data (lznc/map :supply) hamf-sort inorder-dedup)])


(defn ds-java-hashset-sort []
  [(->> ds :name hamf/java-hashset hamf-sort)
   (->> ds :supply hamf/java-hashset hamf-sort)])

(defn set-add!
  [^Set s v] (.add s v) s)


(defn ds-parallel-hashset-union
  []
  (let [rf (fn [data] (->> (hamf-rf/preduce hamf/mut-set set-add! hamf/union data)
                           (hamf-sort)))]
    [(rf (ds :name))
     (rf (ds :supply))]))

(defn take-left
  "Used as a passthrough merge-fn when both sides are equal."
  [l r] l)


(defn ds-parallel-concurrent-hashset
  []
  (let [rf (fn [data]
             (let [s (hamf-set/java-concurrent-hashset)]
               (->> (hamf-rf/preduce (constantly s) set-add! take-left data)
                    (hamf-sort))))]
    [(rf (ds :name))
     (rf (ds :supply))]))


(defn map-singlepass-concurrent-hashset
  []
  (let [make-reducer (fn [k]
                       (let [s (hamf-set/java-concurrent-hashset)]
                         (hamf-rf/parallel-reducer
                          (constantly s)
                          #(set-add! %1 (get %2 k))
                          take-left
                          hamf-sort)))]
    (hamf-rf/preduce-reducers [(make-reducer :name)
                               (make-reducer :supply)]
                              vec-data)))



(defn map-singlepass-hashset-union
  []
  (let [make-reducer (fn [k]
                       (hamf-rf/parallel-reducer
                        hamf/mut-set
                        #(set-add! %1 (get %2 k))
                        hamf/union
                        hamf-sort))]
    (hamf-rf/preduce-reducers [(make-reducer :name)
                               (make-reducer :supply)]
                              vec-data)))


(defn range-reduce-reducers
  [reducers n-elems]
  (let [r (hamf-rf/compose-reducers reducers)
        init-fn (hamf-proto/->init-val-fn r)
        rfn (hamf-proto/->rfn r)]
    (->> (hamf/pgroups
          n-elems
          (fn [^long sidx ^long eidx]
            (rfn (init-fn) [sidx eidx])))
         (reduce (hamf-proto/->merge-fn r))
         (hamf-proto/finalize r))))


(defn map-range-hashset-union
  []
  (let [make-reducer (fn [k]
                       (hamf-rf/parallel-reducer
                        hamf/mut-set
                        #(do (.addAll ^Set %1 (lznc/map k (hamf/subvec vec-data (%2 0) (%2 1))))
                             %1)
                        hamf/union
                        hamf-sort))]
    (range-reduce-reducers [(make-reducer :name)
                            (make-reducer :supply)]
                           (count vec-data))))



(defn ds-cols-custom-singlepass
  []
  (let [make-reducer (fn [col]
                       (let [col (dtype/->buffer col)]
                         (if (= :int32 (dtype/elemwise-datatype col))
                           (hamf-rf/parallel-reducer
                            bitmap/->bitmap
                            (fn [acc rng]
                              (let [sidx (long (rng 0))
                                    eidx (long (rng 1))]
                                (.add ^RoaringBitmap acc (dtype/->int-array (.subBuffer col sidx eidx)))
                                acc))
                            #(do (.or ^RoaringBitmap %1 ^RoaringBitmap %2) %1)
                            bitmap/->random-access)
                           (hamf-rf/parallel-reducer
                            hamf/mut-set
                            (fn [acc rng]
                              (let [sidx (long (rng 0))
                                    eidx (long (rng 1))]
                                (.addAll ^Set acc (.subBuffer col sidx eidx))
                                acc))
                            hamf/union
                            hamf-sort))))]
    (range-reduce-reducers [(make-reducer (ds :name))
                            (make-reducer (ds :supply))]
                           (ds/row-count ds))))



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
          #'parallel-load-dataset
          #'via-reduce-kv
          #'ds-set-sort
          #'hamf-lznc
          #'via-treeset
          #'via-hamf-sort
          #'ds-java-hashset-sort
          #'ds-parallel-hashset-union
          #'ds-parallel-concurrent-hashset
          #'map-singlepass-concurrent-hashset
          #'map-singlepass-hashset-union
          #'ds-cols-custom-singlepass]
         (when @duckdb-conn*
           [#'load-duckdb-data
            #'duckdb-prepared]))]
    (-> (ds/->dataset (mapv bench-it profile-fns))
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
