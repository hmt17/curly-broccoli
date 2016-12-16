package com.cloud;

import java.util.*;

public class ParserReturnable implements Comparable<ParserReturnable>{
    public String filename;
    public int numOfOccur;
    public ArrayList<Integer> offsets;
    
    public ParserReturnable() {
        this.filename = "";
        this.numOfOccur = 0;
        this.offsets = new ArrayList<Integer>();
    }
    
    public ParserReturnable(String fn, int num, ArrayList<Integer> incomingOffsets) {
        this.filename = fn;
        this.numOfOccur = num;
        this.offsets = new ArrayList<Integer>(incomingOffsets);
    }
    
    public int getNumofOccurs()
    {
        return numOfOccur;
    }
    
    public void setNumofOccurs(int numOfOccur)
    {
        this.numOfOccur = numOfOccur;
    }
    
    public String toString() {
        return "filename: " + filename + "\toccurances: " + numOfOccur;
    }
    @Override
    public int compareTo(ParserReturnable pr) {
        int num = this.numOfOccur;
        int otherNum = pr.numOfOccur;
        if(num >= otherNum){
            return 1;
        }
        else{
            return 0;
        }
    }
}
