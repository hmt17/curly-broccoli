import java.util.*;

class ParserReturnable implements Comparable<ParserReturnable>{
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
	
  public int getNumofOccurs()
  {
	  return numOfOccur;
  }
  
  public void setNumofOccurs(int numOfOccur)
  {
	  this.numOfOccur = numOfOccur;
  }
  /*public ParserReturnable getParser(){
	return ParserReturnable;	
  }*/
  public String toString() {
    return "filename: " + filename + "\toccurances: " + numOfOccur + "\toffsets: " + offsets.toString();
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
			//return (this.numOfOccur).compareTo(pr.numOfOccur);
			//return (ParserReturnable(this.numOfOccur)).compareTo((ParserReturnable)pr.numOfOccur);
			//return ((Integer)d.numOfOccur).compareTo(d1.numOfOccur);
        }


}
