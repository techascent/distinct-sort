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

(def vec-data (->> (lznc/repeatedly 1000000 (fn [] {:name (str "some-name-" (rand-int 10000))
                                                    :supply (rand-int 50000)}))
                   (hamf/vec)))
(def obj-data (hamf/object-array-list vec-data))

;;Loading the dataset this way is about twice as fast as simply passing
;;in the vector of maps as the key structure is set at the very beginning.
(defn load-dataset-cwise
  ([] (load-dataset-cwise vec-data))
  ([vec-data] (ds/->dataset {:name (lznc/map :name vec-data)
                             :supply (lznc/map :supply vec-data)})))


(defn parallel-load-dataset-cwise
  []
  (->> (hamf/pgroups
        (count vec-data)
        (fn [^long sidx ^long eidx]
          (load-dataset-cwise (hamf/subvec vec-data sidx eidx))))
       (apply ds/concat)))


(def ds (parallel-load-dataset-cwise))

;; distinct-sort> schema
;; [{:categorical? true, :name :name, :datatype :string, :n-elems 1000000}
;;  {:name :supply, :datatype :int32, :n-elems 1000000}]
(def schema (mapv meta (vals ds)))

(def target-keys (ds/column-names ds))

(defn key-op [f] (mapv f [:name :supply]))

(defn xforms []
  (xforms/transjuxt (key-op (fn [k] (comp (map k) (distinct) (xforms/sort) (xforms/into []))))
                    vec-data))

(defn via-reduce-kv []
  (-> (reduce (fn [eax item]
                    (reduce-kv #(update %1 %2 conj %3) eax item))
                  {} vec-data)
          (update-vals (comp vec sort set))))


(defn ds-set-sort []
  (key-op #(-> ds % set sort vec)))


(defn hamf-sort
  [data]
  ;;Passing in nil for the comparator tells hamf to explicitly avoid
  ;;using the default clojure comparator.  This makes the sort more fragile
  ;;but a bit faster specifically in the case of a stream of integers.
  (hamf/sort nil data))


(defn hamf-lznc []
  (key-op #(->> vec-data (lznc/map %) hamf/java-hashset hamf-sort)))


(defn treeset
  ([] (TreeSet.))
  ([vec-data]
   (doto (TreeSet.)
     (.addAll vec-data))))


(defn via-treeset [] (key-op #(->> vec-data (lznc/map %) treeset)))


(defn inorder-dedup
  [data]
  (mapv #(reduce (fn [acc v] v) nil %) (lznc/partition-by identity data)))


(defn via-hamf-sort
  "Out of curiosity, how does a pure sort of the data followed by an in-order deduplication compare?"
  []
  (key-op #(->> vec-data (lznc/map %) hamf-sort inorder-dedup)))


(defn ds-java-hashset-sort []
  (key-op #(->> ds % hamf/java-hashset hamf-sort)))

(defn set-add! [^Set s v] (.add s v) s)

(defn set-add-all! [^Set s vs] (.addAll s vs) s)

(defn ds-parallel-hashset-union
  []
  (key-op #(->> (hamf-rf/preduce hamf/mut-set set-add! hamf/union (ds %))
                (hamf-sort))))

(defn take-left
  "Used as a passthrough merge-fn when both sides are equal."
  [l r] l)


(defn ds-parallel-concurrent-hashset
  []
  (key-op #(let [s (hamf-set/java-concurrent-hashset)]
             (->> (hamf-rf/preduce (constantly s) set-add! take-left (ds %))
                  (hamf-sort)))))


(defn map-singlepass-concurrent-hashset
  []
  (hamf-rf/preduce-reducers (key-op (fn [k]
                                      (let [s (hamf-set/java-concurrent-hashset)]
                                        (hamf-rf/parallel-reducer
                                         (constantly s)
                                         #(set-add! %1 (get %2 k))
                                         take-left
                                         hamf-sort))))
                            vec-data))



(defn map-singlepass-hashset-union
  []
  (hamf-rf/preduce-reducers (key-op (fn [k]
                                      (hamf-rf/parallel-reducer
                                       hamf/mut-set
                                       #(set-add! %1 (get %2 k))
                                       hamf/union
                                       hamf-sort)))
                            vec-data))


;;It turns out it is generally faster to use .addAll methods when they are available.
;;So to target these we do a parallel reduction over the index space represented as
;;a sequence of tuples of [start-idx end-idx].
(defn range-reduce-reducers
  [reducer-fn n-elems]
  (let [r (hamf-rf/compose-reducers (key-op reducer-fn))
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
  (range-reduce-reducers (fn [k]
                           (hamf-rf/parallel-reducer
                            hamf/mut-set
                            #(set-add-all! %1 (lznc/map k (hamf/subvec vec-data (%2 0) (%2 1))))
                            hamf/union
                            hamf-sort))
                         (count vec-data)))


(defn map-range-concurrent-hashset
  []
  (range-reduce-reducers (fn [k]
                           (let [s (hamf-set/java-concurrent-hashset)]
                             (hamf-rf/parallel-reducer
                              (constantly s)
                              #(set-add-all! %1 (lznc/map k (hamf/subvec vec-data (%2 0) (%2 1))))
                              take-left
                              hamf-sort)))
                         (count vec-data)))



(defn ds-cols-custom-singlepass
  []
  (range-reduce-reducers (fn [k]
                           (let [col (dtype/->buffer (ds k))]
                             (if (= :int32 (dtype/elemwise-datatype col))
                               (hamf-rf/parallel-reducer
                                bitmap/->bitmap
                                ;;roaring bitmaps have an accelerated add method for blocks of integers
                                #(do (.add ^RoaringBitmap %1
                                           ;;the column has an accelerated pathway for copy-to-int-array.
                                           (dtype/->int-array (.subBuffer col (%2 0) (%2 1))))
                                     %1)
                                #(do (.or ^RoaringBitmap %1 ^RoaringBitmap %2) %1)
                                ;;Converts bitmap back into an random access container (:uint32)
                                bitmap/->random-access)
                               (hamf-rf/parallel-reducer
                                hamf/mut-set
                                #(set-add-all! %1 (.subBuffer col (%2 0) (%2 1)))
                                hamf/union
                                hamf-sort))))
                         (ds/row-count ds)))



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
  [conn]
  (try (duckdb/drop-table! conn "_unnamed")
       (catch Exception e))
  (duckdb/create-table! conn ds)
  (duckdb/insert-dataset! conn ds))


(defonce duckdb-table*
  (delay (when @duckdb-conn*
           (println "loading data into duckdb")
           (time
            (load-duckdb-data @duckdb-conn*)))))

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

(defn- run-it
  [x]
  (x)
  {:name (keyword (str (:name (meta x))))
   :mean-ms :ok
   :variance-ms :ok})


(defn run-full-benchmark
  ([] (run-full-benchmark bench-it))
  ([bench-fn]
   (println "running full benchmark ...")
   (let [profile-fns
         (lznc/concat
          [#'xforms
           #'load-dataset-cwise
           #'parallel-load-dataset-cwise
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
           #'map-range-hashset-union
           #'map-range-concurrent-hashset
           #'ds-cols-custom-singlepass]
          (when @duckdb-conn*
            [#'load-duckdb-data
             #'duckdb-prepared]))]
     (-> (ds/->dataset (mapv bench-fn profile-fns))
         (ds/select-columns [:name :mean-ms :variance-ms])
         (ds/sort-by-column :mean-ms)))))


(comment
  (run-full-benchmark run-it)
  )

(defn -main
  [& args]
  (println (run-full-benchmark)))
