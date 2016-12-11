import java.util.*;

class ParserReturnable {
  String filename;
  int numOfOccur;
  ArrayList<Integer> offsets;
  
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
  
  public String toString() {
    return "filename: " + filename + "\toccurances: " + numOfOccur + "\toffsets: " + offsets.toString();
  }
}
