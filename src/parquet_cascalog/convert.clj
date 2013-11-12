(ns parquet-cascalog.convert
  (:import [parquet.schema
            MessageType 
            PrimitiveType PrimitiveType$PrimitiveTypeName
            Type$Repetition]
           org.apache.hadoop.hive.metastore.HiveMetaStoreClient
           org.apache.hadoop.hive.conf.HiveConf)
  (:gen-class
   :methods [^{:static true} [parquetify [String Object] parquet.schema.MessageType]]))

(defn -parquetify
  "Takes some sort of sequence of field names and types.
Returns a parquet.schema.MessageType instance."
  [msg-name fields]
  (let [type-mapping
        {"double" PrimitiveType$PrimitiveTypeName/DOUBLE
         "real" PrimitiveType$PrimitiveTypeName/DOUBLE
         "int" PrimitiveType$PrimitiveTypeName/INT32
         "bigint" PrimitiveType$PrimitiveTypeName/INT64
         "string" PrimitiveType$PrimitiveTypeName/BINARY}]
    (MessageType.
     msg-name
     (map (fn [[field-name type]]
            (PrimitiveType. 
             Type$Repetition/REQUIRED
             (type-mapping type)
             field-name))
          fields))))
