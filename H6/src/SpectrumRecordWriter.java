import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class SpectrumRecordWriter extends RecordWriter<Text, DoubleWritable> {

  public SpectrumRecordWriter() {}

  @Override
  public void write(Text key, DoubleWritable value) {}

  @Override
  public void close(TaskAttemptContext context) {}

}
