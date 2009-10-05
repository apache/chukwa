package org.apache.hadoop.chukwa;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import static org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class ChukwaArchive extends Utf8StorageConverter implements LoadFunc {

  private SequenceFile.Reader r = null;
  private long end = -1;

  private TupleFactory tf = DefaultTupleFactory.getInstance();
  
  @Override
  public void bindTo(String name, BufferedPositionedInputStream arg1,
      long offset, long end) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(name);
    r = new SequenceFile.Reader(fs, path, conf);
    if(offset > 0)
      r.sync(offset);
    this.end = end;
    
//    System.out.println("bound to " + name + " at " + offset);
  }
  

  @Override
  public void fieldsToRead(Schema arg0) {
    //we don't need this; no-op
  }

  
  static Schema chukwaArchiveSchema;
  static int schemaFieldCount;
  static {
    chukwaArchiveSchema = new Schema();
    chukwaArchiveSchema.add(new FieldSchema("seqNo", DataType.LONG));
    chukwaArchiveSchema.add(new FieldSchema("type", DataType.CHARARRAY));
    chukwaArchiveSchema.add(new FieldSchema("name", DataType.CHARARRAY));
    chukwaArchiveSchema.add(new FieldSchema("source", DataType.CHARARRAY));
    chukwaArchiveSchema.add(new FieldSchema("tags", DataType.CHARARRAY));
    chukwaArchiveSchema.add(new FieldSchema("data", DataType.BYTEARRAY));
    schemaFieldCount = chukwaArchiveSchema.size();
    //do we want to expose the record offsets?
  }

  @Override
  public Schema determineSchema(String arg0, ExecType arg1, DataStorage arg2)
      throws IOException {
    return chukwaArchiveSchema;
  }

  @Override
  public Tuple getNext() throws IOException {
    
    ChukwaArchiveKey key = new ChukwaArchiveKey();
    ChunkImpl val = ChunkImpl.getBlankChunk();
    if(r.getPosition() > end || !r.next(key, val)) {
      return null;
    }
    Tuple t = tf.newTuple(schemaFieldCount);
    t.set(0, new Long(val.seqID));
    t.set(1, val.getDataType());
    t.set(2, val.getStreamName());
    t.set(3, val.getSource());
    t.set(4, val.getTags());
    t.set(5, val.getData());
    
//    System.out.println("returning " + t);
    return t;
  }


}
