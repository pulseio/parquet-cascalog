package io.pulse.parquet;

import java.util.List;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.conf.Configuration;
import parquet.schema.MessageType;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.RecordConsumer;
import parquet.io.api.Binary;
import parquet.column.ColumnDescriptor;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

public class TupleEntrySupport extends WriteSupport<TupleEntry> {
  public static final String FIELDS = "io.pulse.fields";
  public static final String TABLE_NAME = "io.pulse.table_name";
  private MessageType schema;
  private List defs;
  private List<ColumnDescriptor> columns;
  private RecordConsumer rc;

  public static Configuration setTable(Configuration conf, String fields, String name) {
    conf.set(FIELDS, fields);
    conf.set(TABLE_NAME, name);
    return conf;
  }

  @Override
  public parquet.hadoop.api.WriteSupport.WriteContext init(Configuration conf) {
    defs = (java.util.List) clojure.lang.RT.readString(conf.get(FIELDS));
    schema = parquet_cascalog.convert.parquetify(conf.get(TABLE_NAME), defs);
    columns = schema.getColumns();
    return new WriteContext(schema, new java.util.HashMap());
  }

  @Override
  public void prepareForWrite(RecordConsumer r) {
    rc = r;
  }

  @Override
  public void write(TupleEntry record) {
    int i;
    rc.startMessage();
    for (i=0; i<columns.size(); i++) {
      ColumnDescriptor column = columns.get(i);
      String fieldName = column.getPath()[0];
      clojure.lang.PersistentVector def = (clojure.lang.PersistentVector) defs.get(i);
      String tupleField = (String) def.get(0);

      rc.startField(fieldName, i);
      switch (column.getType()) {
      case FLOAT:
        rc.addFloat((float) record.getDouble(tupleField));
        break;
      case DOUBLE:
        rc.addDouble(record.getDouble(tupleField));
        break;
      case INT32:
        rc.addInteger((int) record.getLong(tupleField));
        break;
      case INT64:
        rc.addLong(record.getLong(tupleField));
        break;
      case BINARY:
        rc.addBinary(Binary.fromByteArray(record.getString(tupleField).getBytes()));
        break;
      default:
        break;
      }
      rc.endField(fieldName, i);
    }
    rc.endMessage();
  }

}
