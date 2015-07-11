package org.clueweb.ranking;



import java.io.IOException;
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
import org.clueweb.data.TermStatistics;
import org.clueweb.util.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.clueweb.ranking.Score;

public class ComBFScan {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private ComBFScan() {}
  public static  ComKeyValue[] data = new ComKeyValue[1000000000];
  public static int numDocRead = 0;
  public static int numFile = 0;
  private static ComRankerThread [] Thread;
  public static Score [] allScore = new Score[100000];
  public static void main(String[] args) throws IOException {
    if (args.length < 5) {
      System.out.println("args: [document vectors path] [# top documents] [dictionary path] [numThread] [query file]");
      System.exit(-1);
    }

    String f = args[0];
    String queryFile = args[4];
    // load dictionary
    String dictPath = args[2];
    Configuration conf = new Configuration();
    FileSystem fs1 = FileSystem.get(conf); 
    DefaultFrequencySortedDictionary dictionary = new DefaultFrequencySortedDictionary(dictPath, fs1);
    TermStatistics stats = new TermStatistics(new Path(dictPath), fs1);
    // read query and convert to ids
   
    int numDoc;

    int max = Integer.MAX_VALUE;

    FileSystem fs = FileSystem.get(new Configuration());
    Path p = new Path(f);

    if (fs.getFileStatus(p).isDirectory()) {
      numDoc = readSequenceFilesInDir(p, fs, max);
    } else {
      numDoc = readSequenceFile(p, fs, max);
    }
    
    queryTermtoID allQuery = new queryTermtoID(queryFile, dictionary);
    System.out.println("Number of query read: " + allQuery.nq);
    
    // compute ctfs and idfs of query terms
    for(int k = 0; k < allQuery.nq; k++) {
      List<Float> idf = new ArrayList<Float>();
      List<Long> ctf = new ArrayList<Long>();
      for(int l = 0; l < allQuery.query[k].TermID.size(); l++) {
        int id = allQuery.query[k].TermID.get(l);
        int df = stats.getDf(id);
        idf.add((float) Math.log((1.0f*numDoc-df+0.5f)/(df+0.5f)));
        ctf.add(stats.getCf(id));
      }
      allQuery.query[k] = new Query(allQuery.query[k].qno, allQuery.query[k].TermID, idf, ctf);
    }
    
    //getQueryTermIdStat  allQuery = new getQueryTermIdStat(queryFile, dictionary, stats, numDoc);
    
    // initialize score array 
    for(int i = 0; i < allScore.length; i++)
      allScore[i] = new Score("nodoc", -1.0f, 0);
    
    int numTopDoc = Integer.parseInt(args[1]);
    int numThread = Integer.parseInt(args[3]);
    Thread = new ComRankerThread[numThread];
    int segLen = numDoc/numThread;
    
    long startTime = System.nanoTime(); 

    for(int j = 0; j < allQuery.nq; j++) {
      
    for(int i = 0; i < numThread; i++) {
      Thread[i] = new ComRankerThread("Thread", i*segLen, (i+1)*segLen, data, allQuery.query[j], numTopDoc, i, allScore);
      Thread[i].start();
    }
    
    try {
    for(int i = 0; i < numThread; i++)
      Thread[i].Thr.join();
    }
    catch (InterruptedException e) {
      System.out.println("Main thread Interrupted");
   }
    
    Arrays.sort(allScore, new Comparator<Score>() {
      public int compare(Score a, Score b) {
        if(a.score > b.score)
          return -1;
        else if(a.score < b.score)
          return 1;
        else
          return 0;
     }
   });
    
    for(int l = 0; l < numTopDoc; l++) {
      System.out.println(allScore[l].qid + " Q0 " + allScore[l].docid + " " + l + " " + allScore[l].score + " bm25"); 
      allScore[l].score = -1.0f;
    }
    }
    
    long endTime = System.nanoTime(); 
    System.out.println("Time: " + (endTime-startTime)/1000000000.0 + " s");
    System.out.println("# of query: " + allQuery.nq);
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
        data[numDocRead] = new ComKeyValue(key.toString(), value);
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

