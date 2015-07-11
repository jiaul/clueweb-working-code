package org.clueweb.ranking;

import java.io.IOException;

//import edu.umd.cloud9.ranking.Score;
//import edu.umd.cloud9.ranking.RankerThread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import tl.lin.data.array.IntArrayWritable;

import org.clueweb.dictionary.*;
import org.clueweb.data.PForDocVector;
import org.clueweb.data.TermStatistics;

import org.clueweb.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;


public class RecursiveBFScan {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private RecursiveBFScan() {}
  public static  DecomKeyValue[] data = new DecomKeyValue[1000000000];
  public static int numDocRead = 0;
  public static int numFile = 0;
  private static final PForDocVector DOC = new PForDocVector();
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.out.println("args: [path] [# top documents] [dictionary path] [cutoff (for recursive forkjoin)]");
      System.exit(-1);
    }

    String f = args[0];
    String queryFile = "web-13-14.topics";
    // load dictionary
    String dictPath = args[2];
    Configuration conf = new Configuration();
    FileSystem fs1 = FileSystem.get(conf); 
    DefaultFrequencySortedDictionary dictionary = new DefaultFrequencySortedDictionary(dictPath, fs1);
    TermStatistics stats = new TermStatistics(new Path(dictPath), fs1);
    // read query and convert to ids
    queryTermtoID allQuery = new queryTermtoID(queryFile, dictionary);
    System.out.println("Number of query read: " + allQuery.nq);
   
    int numDoc;

    int max = Integer.MAX_VALUE;

    FileSystem fs = FileSystem.get(new Configuration());
    Path p = new Path(f);

    if (fs.getFileStatus(p).isDirectory()) {
      numDoc = readSequenceFilesInDir(p, fs, max);
    } else {
      numDoc = readSequenceFile(p, fs, max);
    }
    
// compute ctfs and idfs of query terms
    for(int k = 0; k < allQuery.nq; k++) {
      List<Float> idf = new ArrayList<Float>();
      List<Long> ctf = new ArrayList<Long>();
      for(int l = 0; l < allQuery.query[k].TermID.size(); l++) {
        int id = allQuery.query[k].TermID.get(l);
        int df = stats.getDf(id);
        idf.add((float) Math.log((1.0f*52000000-df+0.5f)/(df+0.5f)));
        ctf.add(stats.getCf(id));
      }
      allQuery.query[k] = new Query(allQuery.query[k].qno, allQuery.query[k].TermID, idf, ctf);
    }
    
    // recursive forkjoin multithreading
    int cutoff = Integer.parseInt(args[3]);
    int numTopDoc = Integer.parseInt(args[1]);
    
    long startTime = System.nanoTime(); 
    for(int i = 0; i < allQuery.nq; i++) {
      NewDecomForkJoinThread fb = new NewDecomForkJoinThread(0, numDoc, data, allQuery.query[i], cutoff, numTopDoc);
      ForkJoinPool pool = new ForkJoinPool();
      pool.invoke(fb);
    }
    
    long endTime = System.nanoTime(); 
    System.out.print("Time: " + (endTime-startTime)/1000000000.0 + " s\n");
  }
  
  

  private static int readSequenceFile(Path path, FileSystem fs, int max) throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, fs.getConf());

    System.out.println("Reading " + path + "...\n");
    try {
      System.out.println("Key type: " + reader.getKeyClass().toString());
      System.out.println("Value type: " + reader.getValueClass().toString() + "\n");
    } catch (Exception e) {
      throw new RuntimeException("Error: loading key/value class");
    }

    Writable key;
    IntArrayWritable value;
    int n = 0;
    try {
      if ( Tuple.class.isAssignableFrom(reader.getKeyClass())) {
        key = TUPLE_FACTORY.newTuple();
      } else {
        key = (Writable) reader.getKeyClass().newInstance();
      }

      if ( Tuple.class.isAssignableFrom(reader.getValueClass())) {
        value = (IntArrayWritable) TUPLE_FACTORY.newTuple();
      } else {
        value = (IntArrayWritable) reader.getValueClass().newInstance();
      }

      while (reader.next(key, value)) {
        PForDocVector.fromIntArrayWritable(value, DOC);
        data[numDocRead] = new DecomKeyValue(key.toString(), DOC.getTermIds());
        numDocRead++;
        n++;

        if (n >= max)
          break;
      }
      reader.close();
      System.out.println(n + " records read.\n");
    } catch (Exception e) {
      e.printStackTrace();
    }

    return n;
  }

  private static int readSequenceFilesInDir(Path path, FileSystem fs, int max) {
    int n = 0;
    try {
      FileStatus[] stat = fs.listStatus(path);
      for (int i = 0; i < stat.length; ++i) {
        n += readSequenceFile(stat[i].getPath(), fs ,max);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    System.out.println(n + " records read in total.");
    return n;
  }
}