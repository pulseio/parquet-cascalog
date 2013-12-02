(ns parquet-cascalog.hive-tap
  "Don't import this namespace unless you've got Hives."
  (:import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
           org.apache.hadoop.hive.conf.HiveConf)
  (:use parquet-cascalog.tap)
  (:require [cascalog.cascading.tap :as tap]))

(defn get-hive-fields
  "Grab a particular hive table's schema, return a list of fields
   that can be used with hfs-parquet.  Assumes that all query variables
   corresponding to said hive columns will be non-nullable fields.

   Also - will not grab partition columns.  Those need to be handled
   separately.

   If passed a map in the special-cases argument of the form

     {\"foo_column\" foo-function}

   when encountering the hive column \"foo_column\", foo-function will
   be called like (foo-function hive-column-name hive-column-type) and
   is expected to return a vector [cascalog-var type parquet-column-name].
"
  [hive-db hive-table special-cases]
  (let [hive (HiveMetaStoreClient. (HiveConf.))]
    (->> (.. hive (getTable hive-db hive-table) (getSd) (getCols))
         (map (juxt (memfn getName) (memfn getType)))
         (map (fn [[name type]]
                (if-let [f (and special-cases (special-cases name))]
                  (f name type)
                  [(str "?" name) type name]))))))

(defn hfs-hive-parquet
  "Creates an HFS tap that will output to a parquet schema
   appropriate for interoperation with a particular hive
   table.  Forwards any options that would normally apply to
   cascalog.cascading.tap/hfs-tap.

   Also see get-hive-fields."
  [hive-db hive-table path & 
   {:keys [special-cases] :as hfs-options}]
  (apply hfs-parquet
         hive-table
         (get-hive-fields hive-db hive-table special-cases)
         path (mapcat identity (dissoc hfs-options :special-cases))))
