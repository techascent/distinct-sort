# Distinct Sort

Compare various algorithms to achieve a sorted set of multiple columns of data.  Currently the data stored as a vector of maps, a tech.ml.dataset, and a duckdb in-memory table.


## Usage

```console

CHRISs-MBP:distinct-sort chrispnuernberger$ scripts/perftest
Building uberjar

openjdk version "19.0.2" 2023-01-17
OpenJDK Runtime Environment Homebrew (build 19.0.2)
OpenJDK 64-Bit Server VM Homebrew (build 19.0.2, mixed mode, sharing)
running full benchmark ...
Oct 29, 2023 3:51:46 PM clojure.tools.logging$eval136$fn__139 invoke
INFO: Attempting to load duckdb from "/Users/chrispnuernberger/dev/tech.all/distinct-sort/binaries/libduckdb.dylib"
benchmarking :xforms
benchmarking :load-dataset-cwise
benchmarking :parallel-load-dataset-cwise
benchmarking :via-reduce-kv
benchmarking :ds-set-sort
benchmarking :hamf-lznc
benchmarking :via-treeset
benchmarking :via-hamf-sort
benchmarking :ds-java-hashset-sort
benchmarking :ds-parallel-hashset-union
benchmarking :ds-parallel-concurrent-hashset
benchmarking :map-singlepass-concurrent-hashset
benchmarking :map-singlepass-hashset-union
benchmarking :map-range-hashset-union
benchmarking :map-range-concurrent-hashset
benchmarking :ds-cols-custom-singlepass
benchmarking :load-duckdb-data
benchmarking :duckdb-prepared
Oct 29, 2023 3:56:02 PM clojure.tools.logging$eval136$fn__139 invoke
INFO: Reference thread starting
_unnamed [18 3]:

```

|                              :name |     :mean-ms | :variance-ms |
|------------------------------------|-------------:|-------------:|
|                   :duckdb-prepared |  10.13111076 |   0.00076707 |
|         :ds-cols-custom-singlepass |  10.81612538 |   0.00005542 |
|       :parallel-load-dataset-cwise |  12.96070300 |   0.00000046 |
|    :ds-parallel-concurrent-hashset |  20.75493793 |   0.00006997 |
|      :map-range-concurrent-hashset |  22.48023100 |   0.00000740 |
| :map-singlepass-concurrent-hashset |  22.96825737 |   0.00001997 |
|         :ds-parallel-hashset-union |  24.28339487 |   0.00006296 |
|                :load-dataset-cwise |  39.92357317 |   0.00001253 |
|           :map-range-hashset-union |  56.45934617 |   0.00097360 |
|      :map-singlepass-hashset-union |  58.28758933 |   0.00007669 |
|                         :hamf-lznc |  64.34495042 |   0.00027312 |
|                       :ds-set-sort | 105.37044383 |   0.00017169 |
|              :ds-java-hashset-sort | 127.69116567 |   0.00052420 |
|                            :xforms | 175.08987450 |   0.00344701 |
|                     :via-reduce-kv | 180.04342917 |   0.00160358 |
|                     :via-hamf-sort | 200.37273533 |   0.00244236 |
|                       :via-treeset | 328.26164483 |   0.00063673 |
|                  :load-duckdb-data | 560.03121417 |   0.00342610 |

