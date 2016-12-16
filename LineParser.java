package com.cloud;

import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;

public class LineParser {

  public String pathStr;
  public ArrayList<ParserReturnable> parserResult = new ArrayList<ParserReturnable>();
  private static String FULLREGEX = "(.*)-\\{(.*)\\}";
  private static String REGEX = "(\\[([^\\]]+)(?=\\])])";
    Configuration conf = new Configuration();
  
  public LineParser(String path, String key) {
    this.pathStr = path;
    FULLREGEX = "(^" + key.toLowerCase() + ")-\\{(.*)\\}";
  }
  
  public void parse() throws Exception {
    try {
        /* read from HDFS */
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
      
        /* travserse all lines of the output file */
        while ((line = in.readLine()) != null) {
        m.reset(line);
        c = 0;
            /* find matches to the overall regex */
        if(m.find()) {
            /* apply second regex for the filenames and offsets portion of the line */
          String filesWithOffsets = m.group(2);
          r = java.util.regex.Pattern.compile(REGEX);
          m = r.matcher(filesWithOffsets);
          
            /* tracker is keeping track of - in each file portion of each word, you have [filename][numberOfOccurences][offsets] - therefore, tracker keeps track of which we're currently looking at */
          int tracker = 0;
          
          while(m.find()) {
              
            if(tracker == 0) {
                /* filename portion */
              if(c > 0) {
                parserResult.add(pr);
              }
              /* we've moved onto a new file */
              pr = new ParserReturnable();
              pr.filename = m.group(2);
              //System.out.println("filename: " + pr.filename);
            }
            else if(tracker == 1) {
                /* number of occurences portion */
              pr.numOfOccur = Integer.parseInt(m.group(2));
              //System.out.println("numOfOccur: " + pr.numOfOccur);
            }
            else if(tracker == 2) {
                /* list of offsets portion */
              String [] offsets = m.group(2).split(",");
              for(String offset: offsets) {
                pr.offsets.add(Integer.parseInt(offset));
              }
              //System.out.println("offsets: " + pr.offsets);
            }
                        
            c++;
            
              /* reset tracker - new file coming next */
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
    }
    catch(Exception e) {
      System.out.println("Error: " + e.getMessage());
    }
  }

}