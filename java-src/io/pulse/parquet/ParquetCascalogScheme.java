package io.pulse.parquet;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import parquet.hadoop.mapred.Container;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SourceCall;
import cascading.scheme.SinkCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.fs.Path;

import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.Footer;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hadoop.mapred.Container;
import parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import parquet.schema.MessageType;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.Scheme;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.CompositeTap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Tuple;
import cascading.tuple.Fields;


/**
 * A Cascading Scheme that sinks the given tuple into a parquet file based on
 * a type spec that is passed in the form of a printed clojure data structure.
 * 
 */
public class ParquetCascalogScheme<T> extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]>{

  private static final long serialVersionUID = 157560846420730048L;
  private String fields;
  private String table;

  public ParquetCascalogScheme(String columns, String table) {
    this.table = table;
    this.fields = columns;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean source(FlowProcess<JobConf> fp, SourceCall<Object[], RecordReader> sc)
      throws IOException {
    throw new IOException("This scheme cannot be used as a source!");
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void sourceConfInit(FlowProcess<JobConf> arg0,
      Tap<JobConf, RecordReader, OutputCollector> arg1, JobConf arg2) {
    throw new UnsupportedOperationException("This scheme cannot be used as a source!");
  }

  @Override
  public void sink(FlowProcess<JobConf> fp, SinkCall<Object[], OutputCollector> sc)
      throws IOException {
    TupleEntry tuple = sc.getOutgoingEntry();
    OutputCollector output = sc.getOutput();
    output.collect(null, tuple);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void sinkConfInit(FlowProcess<JobConf> fp,
                           Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {

    jobConf.setOutputFormat(DeprecatedParquetOutputFormat.class);
    DeprecatedParquetOutputFormat.setWriteSupportClass(jobConf, TupleEntrySupport.class);
    TupleEntrySupport.setTable(jobConf, fields, table);
  }

}
