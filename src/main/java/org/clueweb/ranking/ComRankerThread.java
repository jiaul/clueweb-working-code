package org.clueweb.ranking;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.clueweb.ranking.Score;
import org.clueweb.util.*;
import org.clueweb.data.*;

public class ComRankerThread implements Runnable {
  public Thread Thr;
  private String threadName;
  private int start, end;
  private ComKeyValue[] data;
  private Query query;
  private int numTopDoc;
  private int qlen;
  private int thid;
  private Score [] allScore;
  private int[] tf = new int[10];
  private final MTPForDocVector doc = new MTPForDocVector();
  
  public ComRankerThread(String name, int start, int end, ComKeyValue[] data, Query query, int numTopDoc, int thid, Score [] allScore){
      threadName = name;
      this.start = start;
      this.end = end;
      this.data = data;
      this.numTopDoc = numTopDoc;
      this.query = query;
      this.thid = thid;
      this.allScore = allScore;
      qlen = query.TermID.size();
  }
  
  public void run() {
    float score;
    PriorityQueue<Score> scoreQueue = new PriorityQueue<Score>(numTopDoc, new Comparator<Score>() {
      public int compare(Score a, Score b) {
         if(a.score < b.score)
           return -1;
         else
           return 1;
      }
    });
    
    if(numTopDoc > 200)
      numTopDoc = 200;
    int n = 0;
    int dlen = 0;
     for(int i = start; i < end; i++) {
       Arrays.fill(tf, 0);
       doc.fromIntArrayWritable(data[i].doc, doc);
       dlen = doc.getLength();
       
       /*System.out.println(data[i].doc.toString());
       System.out.print("DOCUMENT " + data[i].key + " " + dlen);
       for(int termid : doc.getTermIds())
         System.out.print(" " + termid);
       System.out.println("");*/
       
       for (int termid : doc.getTermIds()) {
         for(int j = 0; j < qlen; j++)
           if(query.TermID.get(j) == termid)
             tf[j]++;
       }
       score = 0.0f;
       float k1 = 1.0f;
       float b = 0.5f;
       float adl = 450.0f;
       for(int k = 0; k < qlen; k++)
         score += query.idf.get(k) * ((k1+1.0f) * tf[k])/(k1*(1.0f-b+b*dlen/adl)+tf[k]);
       
       
       if(n < numTopDoc) {
         scoreQueue.add(new Score(data[i].key, score, query.qno));
         n++;
       }
       else {
         if(scoreQueue.peek().score < score) {
           scoreQueue.poll();
           scoreQueue.add(new Score(data[i].key, score, query.qno));
         }
       }
     }
     // print top 10 results
     int scoreQSize = Math.min(n, scoreQueue.size());
     int spos = numTopDoc * thid;
     for(int k = 0; k < scoreQSize; k++) {
       Score temp = scoreQueue.poll();
       allScore[spos] = new Score(temp.docid, temp.score, temp.qid);
       spos++;
     }
  }
  
  public void start ()
  {
     if (Thr == null)
     {
        Thr = new Thread (this, threadName);
        Thr.start ();
     }
  }


  public static void main(String args[]) {
  
     int[] myList = new int[10];
     for(int i = 0; i < 10; i++)
       myList[i] = i;
   /*  ComRankerThread R1 = new ComRankerThread( "Thread-1", 0, 5, myList);
     R1.start();
     
     ComRankerThread R2 = new ComRankerThread( "Thread-2", 5, 10, myList);
     R2.start(); */
  }   
}