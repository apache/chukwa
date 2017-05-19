package org.apache.hadoop.chukwa.caffe;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.imageio.ImageIO;


/**
 * Read csv files to create image files of dimension 1000 * 200
 * 
 */
public class ImageCreator
{
  private static final int X_SIZE = 1000;
  private static final int Y_SIZE = 200;
  private String dirName = null;
  
  public ImageCreator (String dirName) {
    this.dirName = dirName;
  }
    
  public void drawImages () throws Exception
  {
    String outputFileName = dirName + "/labels.txt";
    BufferedWriter bufferedWriter = null;
    try {
      FileWriter fileWriter = new FileWriter(outputFileName);
      bufferedWriter = new BufferedWriter(fileWriter);
    } catch (IOException e) {
      e.printStackTrace ();
    }
       
    //int start = 1;
    File dir = new File (dirName);
    File [] files = dir.listFiles ();
    Arrays.sort(files);
    
    // find min and max memory usage
    double minMem = 0;
    double maxMem = 0;
    long minTime = 0L;
    long maxTime = 0L;
    
    // image size: 1000 *200
    int lineNum = 0;
    for (int i = 0; i < files.length; i++) {
      String fileName = files [i].getName ();
      if (!fileName.endsWith ("csv")) {
        continue;
      }
      //System.out.println (">>>>> " + fileName);
      BufferedReader bufferedReader = new BufferedReader(new FileReader(files [i]));
      String line = null;

      while ((line = bufferedReader.readLine()) != null)
      {
        lineNum ++;
        String [] point = line.split (",");
        long time = Long.parseLong (point[0]);
        double mem = Double.parseDouble (point[1]);
        point [1] = String.valueOf (mem);
        if (maxMem == 0 || maxMem < mem){
          maxMem = mem;
        }
        if (minMem == 0 || minMem > mem) {
          minMem = mem;
        }
        if (maxTime == 0 || maxTime < time){
          maxTime = time;
        }
        if (minTime == 0 || minTime > time) {
          minTime = time;
        }        
      }
      bufferedReader.close ();
    }
    //System.out.println ("minMem:" + minMem + ", maxMem:" + maxMem + ", total line number: " + lineNum);
    //System.out.println ("minTime:" + minTime + ", maxTime:" + maxTime + ", total elapseTime: " + (maxTime - minTime));
    
    List <String []> dataList = new ArrayList<String []> ();
    lineNum = 0;
    long startTime = 0;
    long endTime = 0;
    int imageId = 1;
    int totalPoint = 0;
    for (int i = 0; i < files.length; i++) {
      String fileName = files [i].getName ();
      if (!fileName.endsWith ("csv")) {
        continue;
      }
      System.out.println (">>>>> " + fileName);
      BufferedReader bufferedReader = new BufferedReader(new FileReader(files [i]));
      String line = null;

      while ((line = bufferedReader.readLine()) != null)
      {
        lineNum ++;
        String [] point = line.split (",");
        long time = Long.parseLong (point[0]);
        double mem = Double.parseDouble (point[1]);
        point [1] = String.valueOf (mem);

        if (startTime == 0) {
          startTime = time;
        } 
        dataList.add (point);        
        endTime = time;
        long elapseTime = endTime - startTime;
        if (elapseTime > X_SIZE) {
          totalPoint = totalPoint + dataList.size ();
          String imageFileName = dirName + "\\image" + imageId + ".png";
          System.out.println ("elapseTime: " + elapseTime + ", data size: " + dataList.size () + ", imageFileName: " + imageFileName);
          drawImage (dataList, imageFileName, X_SIZE, Y_SIZE);
          bufferedWriter.write (imageFileName + " 0\n");
          bufferedWriter.flush ();
          dataList.clear ();
          startTime = 0;
          imageId ++;
        }
      }
      bufferedReader.close ();
      bufferedWriter.close ();
    }
    //System.out.println ("Total points: " + totalPoint + ", lineNum: " + lineNum);
  }
  
  private static void drawImage (List <String []> dataList, String imageFileName, int x_size, int y_size) throws Exception
  {
    int size = dataList.size ();
    String [] startPt = dataList.get (0);
    //String [] endPt = dataList.get (size - 1);
    long startTimeX = Long.parseLong (startPt [0]);
    //long endTimeX = Long.parseLong (endPt [0]);
    //System.out.println ("x_size: " + x_size + ", y_size: " + y_size + ", startTimeX: " + startTimeX + ", endTimeX: " + endTimeX);
    BufferedImage img = new BufferedImage(x_size, y_size, BufferedImage.TYPE_INT_ARGB);

    Graphics2D ig2 = img.createGraphics();
    ig2.setBackground(Color.WHITE);

    ig2.setColor (Color.BLACK);
    ig2.setStroke(new BasicStroke(3));

    MyPoint prevPoint = null;
    for (int i = 0; i < size; i++) {
      String [] point = (String []) dataList.get (i);
      long time = Long.parseLong (point[0]);
      double mem = Double.parseDouble (point[1]);
      MyPoint currPoint = new MyPoint (time, mem);
      //System.out.println ("time:" + time + ", mem:" + mem);
      
      if (prevPoint != null) {
        ig2.drawLine ((int) (prevPoint.time - startTimeX), (int) (y_size - prevPoint.data), (int) (currPoint.time - startTimeX), (int) (y_size - currPoint.data));
      }  
      prevPoint = currPoint;
    }
    File f = new File(imageFileName);
    ImageIO.write(img, "PNG", f);
  }
}

class MyPoint 
{
  public long time;
  public double data;

  public MyPoint (long time, double data) {
    this.time = time;
    this.data = data;
  }
}