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
Oct 27, 2023 12:59:56 PM clojure.tools.logging$eval136$fn__139 invoke
INFO: Attempting to load duckdb from "/Users/chrispnuernberger/dev/tech.all/distinct-sort/binaries/libduckdb.dylib"
benchmarking :xforms
benchmarking :load-dataset
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
benchmarking :ds-cols-custom-singlepass
benchmarking :load-duckdb-data
benchmarking :duckdb-prepared
Oct 27, 2023 1:03:42 PM clojure.tools.logging$eval136$fn__139 invoke
INFO: Reference thread starting
_unnamed [15 3]:
```
|                              :name |     :mean-ms | :variance-ms |
|------------------------------------|-------------:|-------------:|
|         :ds-cols-custom-singlepass |   7.11180449 |   0.00001037 |
|                   :duckdb-prepared |  10.91315663 |   0.00018282 |
|    :ds-parallel-concurrent-hashset |  18.47679300 |   0.00004757 |
|         :ds-parallel-hashset-union |  21.68379897 |   0.00016757 |
| :map-singlepass-concurrent-hashset |  22.41099627 |   0.00005195 |
|              :ds-java-hashset-sort |  55.57742267 |   0.00031383 |
|                         :hamf-lznc |  56.22949558 |   0.00010750 |
|      :map-singlepass-hashset-union |  62.65949558 |   0.00182414 |
|                       :ds-set-sort |  91.68817625 |   0.00011745 |
|                     :via-hamf-sort | 117.03436000 |   0.00748619 |
|                      :load-dataset | 158.71502667 |   0.00080619 |
|                            :xforms | 176.51129067 |   0.00029054 |
|                     :via-reduce-kv | 231.97556167 |   0.00072451 |
|                       :via-treeset | 357.34842950 |   0.00346563 |
|                  :load-duckdb-data | 409.62395733 |   0.00474322 |

