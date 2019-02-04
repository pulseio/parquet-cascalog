(defproject io.pulse/parquet-cascalog "0.1.0"
  :description "Scheme for sinking cascalog jobs into parquet"
  :url "http://github.com/pulseio/parquet-cascalog/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [cascalog "2.0.0"]
                 [com.twitter/parquet-column "1.2.4"]
                 [com.twitter/parquet-hadoop "1.2.4"]
                 [cascading/cascading-core "2.2.0"]]
  :prep-tasks ["compile" "javac"]
  :aliases {"build" ["do" "compile," "javac"]}
  :java-source-paths ["java-src"]
  :aot [parquet-cascalog.convert]
  :repositories [["conjars" "https://conjars.org/repo"]]
  :profiles {:dev {:dependencies [[org.apache.hadoop/hadoop-core "1.1.2"]
                                  [javax.jdo/jdo2-api "2.3-eb"]
                                  [org.apache.hive/hive-metastore "0.12.0"
                                   :exclusions [javax.jdo/jdo2-api]]]}})
