import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;

class LineParser {

  String pathStr;
  ArrayList<ParserReturnable> parserResult = new ArrayList<ParserReturnable>();
  private static String FULLREGEX = "(.*)-\\{(.*)\\}";
  private static String REGEX = "(\\[([^\\]]+)(?=\\])])";
    Configuration conf = new Configuration();
  
  public LineParser(String path, String key) {
    this.pathStr = path;
    FULLREGEX = "(" + key + ")-\\{(.*)\\}";
  }
  
  public void parse() throws Exception {
    try {
        /*
      FileInputStream fstream = new FileInputStream(path);
      ObjectInputStream objInput = new ObjectInputStream(fstream);
      DataInputStream in = new DataInputStream(objInput);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));*/
        FileSystem fileSystem = FileSystem.get(conf);
        
        Path path = new Path(pathStr);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + path + " does not exist");
            return;
        }
        FSDataInputStream in = fileSystem.open(path);

      String line = "";
      java.util.regex.Pattern r = java.util.regex.Pattern.compile(FULLREGEX);
      java.util.regex.Matcher m = r.matcher("");
      ParserReturnable pr = new ParserReturnable();
      int c = 0;
      
      //while ((line = br.readLine()) != null) {
        while ((line = in.readLine()) != null) {
        m.reset(line);
        c = 0;
        if(m.find()) {
          String filesWithOffsets = m.group(2);
          r = java.util.regex.Pattern.compile(REGEX);
          m = r.matcher(filesWithOffsets);
          
          int tracker = 0;
          
          while(m.find()) {
            if(tracker == 0) {
              if(c > 0) {
                parserResult.add(pr);
              }
              /* we've moved onto a new file */
              pr = new ParserReturnable();
              pr.filename = m.group(2);
              //System.out.println("filename: " + pr.filename);
            }
            else if(tracker == 1) {
              pr.numOfOccur = Integer.parseInt(m.group(2));
              //System.out.println("numOfOccur: " + pr.numOfOccur);
            }
            else if(tracker == 2) {
              String [] offsets = m.group(2).split(",");
              for(String offset: offsets) {
                pr.offsets.add(Integer.parseInt(offset));
              }
              //System.out.println("offsets: " + pr.offsets);
            }
            
            /*for(int i = 0; i <= count; i++) {
             System.out.println("group " + i + ": " + m.group(i));
             }
             System.out.println("start: " + m.start());
             System.out.println("end: " + m.end());
             */
            
            c++;
            
            if(tracker == 2) {
              tracker = 0;
            }
            else {
              tracker++;
            }
          }
        }
        
        if(c == 0) {
          //System.out.println("no matches");
        }
        else {
          parserResult.add(pr);
        }
      }
      
      //System.out.println("Result: " + parserResult.toString());
    }
    catch(Exception e) {
      System.out.println("Error: " + e.getMessage());
    }
  }

}
