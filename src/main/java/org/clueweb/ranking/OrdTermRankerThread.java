package org.clueweb.ranking;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.clueweb.ranking.Score;
import org.clueweb.util.*;

public class OrdTermRankerThread implements Runnable {
  public Thread Thr;
  private String threadName;
  private int start, end;
  private Query query;
  private int numTopDoc;
  private int qlen;
  private int thid;
  private int [][] docs;
  private int [][] freq;
  private String [] keys;
  private Score [] allScore;
  private int [] doclen;
  private int[] qtid = new int[100];
  private double[] idf =  new double[100];
  
  public OrdTermRankerThread(String name, int start, int end, String [] keys, int [][] docs, int [][] freq, int [] doclen, Query query, int numTopDoc, int thid, Score [] allScore){
      threadName = name;
      this.start = start;
      this.end = end;
      this.keys = keys;
      this.docs = docs;
      this.freq = freq;
      this.doclen = doclen;
      this.numTopDoc = numTopDoc;
      this.query = query;
      this.thid = thid;
      this.allScore = allScore;
      qlen = query.TermID.size();
      for(int i = 0; i < qlen; i++) {
    	  idf[i] = query.idf.get(i);
    	  qtid[i] = query.TermID.get(i);
      }
  }
  
  public void run() {
    PriorityQueue<Score> scoreQueue = new PriorityQueue<Score>(numTopDoc, new Comparator<Score>() {
      public int compare(Score a, Score b) {
         if(a.score < b.score)
           return -1;
         else
           return 1;
      }
    });
   // System.out.println("Name: " +  threadName );
    if(numTopDoc > 200)
      numTopDoc = 200;
    int n = 0;
    float k1 = 1.0f;
    float b = 0.5f;
    float adl = 450.0f;
    float score = 0.0f;
     for(int i = start; i < end; i++) {
      score = 0.0f; 
      if(qlen == 1) {
    	  int indx = Arrays.binarySearch(docs[i], qtid[0]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[0] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
        	 }
      }
      else if(qlen == 2) {
    	  int indx = Arrays.binarySearch(docs[i], qtid[0]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[0] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
        	 }
    	  indx = Arrays.binarySearch(docs[i], qtid[1]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[1] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
        	 }
      }
      else if(qlen == 3) {
    	  int indx = Arrays.binarySearch(docs[i], qtid[0]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[0] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
        	 }
    	  indx = Arrays.binarySearch(docs[i], qtid[1]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[1] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
          }
    	  indx = Arrays.binarySearch(docs[i], qtid[2]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[2] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
          }
      }
      else if(qlen == 4) {
    	  int indx = Arrays.binarySearch(docs[i], qtid[0]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[0] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
        	 }
    	  indx = Arrays.binarySearch(docs[i], qtid[1]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[1] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
          }
    	  indx = Arrays.binarySearch(docs[i], qtid[2]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[2] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
          }
    	  indx = Arrays.binarySearch(docs[i], qtid[3]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[3] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
          }
      }
      else if(qlen == 5) {
    	  int indx = Arrays.binarySearch(docs[i], qtid[0]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[0] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
        	 }
    	  indx = Arrays.binarySearch(docs[i], qtid[1]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[1] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
          }
    	  indx = Arrays.binarySearch(docs[i], qtid[2]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[2] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
          }
    	  indx = Arrays.binarySearch(docs[i], qtid[3]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[3] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
          }
    	  indx = Arrays.binarySearch(docs[i], qtid[4]);
    	  if(indx >= 0) {
              int tf = freq[i][indx];
              score += idf[4] * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
          }
      }
      else {
        for(int j = 0; j < qlen; j++) {
       	   int indx = Arrays.binarySearch(docs[i], query.TermID.get(j));
       	   if(indx >= 0) {
             int tf = freq[i][indx];
             score += query.idf.get(j) * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*doclen[i]/adl)+tf);
       	   }
        }
      }
      
      if(score <= 0.0f)
    	  continue;
      
      if(n < numTopDoc) {
         scoreQueue.add(new Score(keys[i], score, query.qno));
         n++;
      }
       else {
         if(scoreQueue.peek().score < score) {
           scoreQueue.poll();
           scoreQueue.add(new Score(keys[i], score, query.qno));
         }
       }
     }
     // take top k results
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
   /*  OrdTermRankerThread R1 = new OrdTermRankerThread( "Thread-1", 0, 5, myList);
     R1.start();
     
     OrdTermRankerThread R2 = new OrdTermRankerThread( "Thread-2", 5, 10, myList);
     R2.start(); */
  }   
}