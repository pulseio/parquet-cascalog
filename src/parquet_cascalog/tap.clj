(ns parquet-cascalog.tap
  (:require [cascalog.cascading.tap :as tap]))

(defn hfs-parquet
  "Creates an HFS tap that will output to a parquet schema
   defined by the supplied field descriptions.  The fields
   argument should be of the form:

     fields     ::= [[<field-name> <field-type> <column-name>?] * ...]
     field-name ::= <string>
     field-type ::= real | double | int | bigint | string"
  [name fields path & options]
  (apply tap/hfs-tap
         (io.pulse.parquet.ParquetCascalogScheme.
          (pr-str fields) name)
         path options))
