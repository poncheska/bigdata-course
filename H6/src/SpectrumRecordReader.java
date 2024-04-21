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

public class SpectrumRecordReader extends RecordReader<Text, RawSpectrum> {

private LineReader[] in;
private Text key;
private RawSpectrum value = new RawSpectrum();
private long start =0;
private long end =0;
private long pos =0;
private int maxLineLength;
private int i = 0; 
private int j = 1;
private  int k = 2; 
private int w = 3; 
private int d = 4;
private Map<String, float[]> is= new HashMap<String, float[]>();
private Map<String, float[]> js = new HashMap<String, float[]>();
private Map<String, float[]> ks = new HashMap<String, float[]>();
private Map<String, float[]> ws = new HashMap<String, float[]>();
private Map<String, float[]> ds = new HashMap<String, float[]>();
Set<String> keyss;
Iterator<String> iter;

  // line example: 2012 01 01 00 00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   ...
  private static final Pattern LINE_PATTERN =
      Pattern.compile("([0-9]{4} [0-9]{2} [0-9]{2} [0-9]{2} [0-9]{2})(.*)");

  @Override
  public void close()  throws IOException {
    //for (LineReader in_reader : in) {
    //  if (in_reader != null) {
    //    in_reader.close();
    //  }
    //}
  }

  @Override
  public Text getCurrentKey() {
    return key;
  }

  @Override
  public RawSpectrum getCurrentValue()  throws IOException {
/*
добавить в rawspectrum
public void setField2(String field, float[] val) {
  	String field = matcher.group(2);
  	if (field.equals("i")) i = val;
  	if (field.equals("j")) j = val;
  	if (field.equals("k")) k = val;
  	if (field.equals("w")) w = val;
  	if (field.equals("d")) d = val;
  }
*/
String t = key.toString();
value.setField2("i", is.get(t));
value.setField2("j", js.get(t));
value.setField2("k", ks.get(t));
value.setField2("w", ws.get(t));
value.setField2("d", ds.get(t));
    return value;
  }

  @Override
  public float getProgress() {
     if (start == end) {
            return 0.0f;
        }
        else {
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }

  }

  @Override
  public void initialize(InputSplit generic_split, TaskAttemptContext context) throws IOException {
    CombineFileSplit split = (CombineFileSplit) generic_split;

    Configuration conf = context.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    FSDataInputStream[] filesin;

    for (Path path : split.getPaths()) {
        FSDataInputStream filein = fs.open(path);
        GZIPInputStream gzis = new GZIPInputStream(filein );
        LineReader in = new LineReader(gzis, conf);
int off = 0;
String res;
do {
		Text txt = new Text();
		off = 0;
		off = in.readLine(txt);
                res = txt.toString().replaceAll("\\s+", " ").trim();
		Matcher matcher = LINE_PATTERN.matcher(res);
		while (!matcher.find()) {
			off = in.readLine(txt);
                        res = txt.toString().replaceAll("\\s+", " ").trim();
                        if (off == 0) break;
                        matcher = LINE_PATTERN.matcher(res);
		}
if (off == 0) break;
res = res.trim();
String key = res.substring(0, 16);

String v = res.substring(16);


List<String> valuesstr = new ArrayList<String>();
 Matcher m = Pattern.compile("[^\\s]+").matcher(v);

 while (m.find()) {
   valuesstr.add(m.group());
 }

//String[] valuesstr = v.replaceAll("\\s+", " ").trim().split(" ");
float[] values = new float[valuesstr.size()];

for (int i = 0; i < valuesstr.size(); i++) {
        //if (valuesstr[i].length() == 0) continue;
	values[i] = Float.parseFloat(valuesstr.get(i));
}
String name = path.getName();
if (name.contains("i")) is.put(key, values);
if (name.contains("j")) js.put(key, values);
if (name.contains("k")) ks.put(key, values);
if (name.contains("w")) ws.put(key, values);
if (name.contains("d")) ds.put(key, values);
} while (off != 0);

keyss = is.keySet();
keyss.removeIf(p -> !js.keySet().contains(p));
keyss.removeIf(p -> !ks.keySet().contains(p));
keyss.removeIf(p -> !ws.keySet().contains(p));
keyss.removeIf(p -> !ds.keySet().contains(p));
iter = keyss.iterator();
    }
    this.pos = start;
  }


  @Override
  public boolean nextKeyValue() {
        if (iter.hasNext()) {
	//key.set(iter.next());
           key = new Text(iter.next());
	return true;
} else {
	return false;
}

  }

  // TODO
}
