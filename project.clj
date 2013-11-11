(defproject pail-test "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [backtype/dfs-datastores "1.2.0"]
                 [cascalog "2.0.0"]
                 [cascading/cascading-core "2.2.0"]
                 [backtype/dfs-datastores-cascading "1.3.0"
                  :exclusions [cascading/cascading-core cascading/cascading-hadoop]]]
  :aot [pail-test.core]
  :repositories [["conjars" "http://conjars.org/repo"]]
  :profiles {:dev {:dependencies [[org.apache.hadoop/hadoop-core "1.1.2"]]}})
