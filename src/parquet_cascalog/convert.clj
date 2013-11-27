(ns parquet-cascalog.convert
  (:import [parquet.schema
            MessageType 
            PrimitiveType PrimitiveType$PrimitiveTypeName
            Type$Repetition])
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
         "integer" PrimitiveType$PrimitiveTypeName/INT32
         "bigint" PrimitiveType$PrimitiveTypeName/INT64
         "string" PrimitiveType$PrimitiveTypeName/BINARY}]
    (MessageType.
     msg-name
     (map (fn [[field-name type column-name]]
            (PrimitiveType. 
             Type$Repetition/REQUIRED
             (type-mapping type)
             (or column-name field-name)))
          fields))))
