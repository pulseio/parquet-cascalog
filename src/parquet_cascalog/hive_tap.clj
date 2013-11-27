(ns parquet-cascalog.hive-tap
  (:import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
           org.apache.hadoop.hive.conf.HiveConf)
  (:use parquet-cascalog.tap)
  (:require [cascalog.cascading.tap :as tap]))

(defn get-hive-fields
  "Grab a particular hive table's schema, return a list of fields
   that can be used with hfs-parquet.  Assumes that all query variables
   corresponding to said hive columns will be non-nullable fields.

   Also - will not grab partition columns.  Those need to be handled
   separately."
  [hive-db hive-table]
  (let [hive (HiveMetaStoreClient. (HiveConf.))]
    (->> (.. hive (getTable hive-db hive-table) (getSd) (getCols))
         (map (juxt (memfn getName) (memfn getType)))
         (map (fn [[name type]] [(str "?" name) type name]))
         pr-str)))

(defn hfs-hive-parquet
  "Creates an HFS tap that will output to a parquet schema
   appropriate for interoperation with a particular hive
   table.  Forwards any options that would normally apply to
   cascalog.cascading.tap/hfs-tap.

   Also see get-hive-fields."
  [hive-db hive-table path & hfs-options]
  (hfs-parquet
   (get-hive-fields hive-db hive-table)
   path hfs-options))
