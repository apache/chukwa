/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.chukwa.hicc;

import java.awt.Component;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.RenderingHints;
import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.awt.image.CropImageFilter;
import java.awt.image.FilteredImageSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.chukwa.util.XssFilter;

public class ImageSlicer {
  private BufferedImage src = null;
  private Log log = LogFactory.getLog(ImageSlicer.class);
  private String sandbox = System.getenv("CHUKWA_HOME")+File.separator+"webapps"+File.separator+"sandbox"+File.separator;
  private int maxLevel = 0;
  
  public ImageSlicer() {
  }
  
  /*
   * Prepare a large image for tiling.
   * 
   * Load an image from a file. Resize the image so that it is square,
   * with dimensions that are an even power of two in length (e.g. 512,
   * 1024, 2048, ...). Then, return it.
   * 
   */
  public BufferedImage prepare(String filename) {
    try {
      src = ImageIO.read(new File(filename));
    } catch (IOException e) {
      log.error("Image file does not exist:"+filename+", can not render image.");
    }
    XYData fullSize = new XYData(1, 1);
    while(fullSize.getX()<src.getWidth() || fullSize.getY()<src.getHeight()) {
      fullSize.set(fullSize.getX()*2, fullSize.getY()*2);
    }
    float scaleX = (float)fullSize.getX()/src.getWidth();
    float scaleY = (float)fullSize.getY()/src.getHeight();
    log.info("Image size: ("+src.getWidth()+","+src.getHeight()+")");
    log.info("Scale size: ("+scaleX+","+scaleY+")");
    
    AffineTransform at = 
      AffineTransform.getScaleInstance(scaleX,scaleY);

      //       AffineTransform.getScaleInstance((fullSize.getX()-src.getWidth())/2,(fullSize.getY()-src.getHeight())/2);
    AffineTransformOp op = new AffineTransformOp(at, AffineTransformOp.TYPE_BILINEAR);
    BufferedImage dest = op.filter(src, null);
    return dest;
  }
  
  /*
   * Extract a single tile from a larger image.
   * 
   * Given an image, a zoom level (int), a quadrant (column, row tuple;
   * ints), and an output size, crop and size a portion of the larger
   * image. If the given zoom level would result in scaling the image up,
   * throw an error - no need to create information where none exists.
   * 
   */
  public BufferedImage tile(BufferedImage image, int level, XYData quadrant, XYData size, boolean efficient) throws Exception {
    double scale = Math.pow(2, level);
    if(efficient) {
      /* efficient: crop out the area of interest first, then scale and copy it */
      XYData inverSize = new XYData((int)(image.getWidth(null)/(size.getX()*scale)), 
          (int)(image.getHeight(null)/(size.getY()*scale)));
      XYData topLeft = new XYData(quadrant.getX()*size.getX()*inverSize.getX(), 
          quadrant.getY()*size.getY()*inverSize.getY());
      XYData newSize = new XYData((size.getX()*inverSize.getX()), 
          (size.getY()*inverSize.getY()));
      if(inverSize.getX()<1.0 || inverSize.getY() < 1.0) {
        throw new Exception("Requested zoom level ("+level+") is too high.");
      }
      image = image.getSubimage(topLeft.getX(), topLeft.getY(), newSize.getX(), newSize.getY());
      BufferedImage zoomed = new BufferedImage(size.getX(), size.getY(), BufferedImage.TYPE_INT_RGB);
      zoomed.getGraphics().drawImage(image, 0, 0, size.getX(), size.getY(), null);
      if(level>maxLevel) {
        maxLevel = level;
      }
      return zoomed;
    } else {
      /* inefficient: copy the whole image, scale it and then crop out the area of interest */
      XYData newSize = new XYData((int)(size.getX()*scale), (int)(size.getY()*scale));
      XYData topLeft = new XYData(quadrant.getX()*size.getX(), quadrant.getY()*size.getY());
      if(newSize.getX() > image.getWidth(null) || newSize.getY() > image.getHeight(null)) {
        throw new Exception("Requested zoom level ("+level+") is too high.");
      }
      AffineTransform tx = new AffineTransform();
      AffineTransformOp op = new AffineTransformOp(tx, AffineTransformOp.TYPE_BILINEAR);
      tx.scale(scale, scale);
      image = op.filter(image, null);
      BufferedImage zoomed = image.getSubimage(topLeft.getX(), topLeft.getY(), newSize.getX(), newSize.getY());
      if(level>maxLevel) {
        maxLevel = level;
      }
      return zoomed;
    }
  }
  
  /*
   * Recursively subdivide a large image into small tiles.
   * 
   * Given an image, a zoom level (int), a quadrant (column, row tuple;
   * ints), and an output size, cut the image into even quarters and
   * recursively subdivide each, then generate a combined tile from the
   * resulting subdivisions. If further subdivision would result in
   * scaling the image up, use tile() to turn the image itself into a
   * tile.
   */
  public BufferedImage subdivide(BufferedImage image, int level, XYData quadrant, XYData size, String prefix) {
    if(image.getWidth()<=size.getX()*Math.pow(2, level)) {
      try {
        BufferedImage outputImage = tile(image, level, quadrant, size, true);
        write(outputImage, level, quadrant, prefix);
        return outputImage;
      } catch (Exception e) {
        log.error(ExceptionUtil.getStackTrace(e));
      }
    }
    
    BufferedImage zoomed = new BufferedImage(size.getX()*2, size.getY()*2, BufferedImage.TYPE_INT_RGB);
    Graphics g = zoomed.getGraphics();
    XYData newQuadrant = new XYData(quadrant.getX() * 2 + 0, quadrant.getY() * 2 + 0);
    g.drawImage(subdivide(image, level+1, newQuadrant, size, prefix), 0, 0, null);
    newQuadrant = new XYData(quadrant.getX()*2 + 0, quadrant.getY()*2 + 1);
    g.drawImage(subdivide(image, level+1, newQuadrant, size, prefix), 0, size.getY(), null);
    newQuadrant = new XYData(quadrant.getX()*2 + 1, quadrant.getY()*2 + 0);
    g.drawImage(subdivide(image, level+1, newQuadrant, size, prefix), size.getX(), 0, null);
    newQuadrant = new XYData(quadrant.getX()*2 + 1, quadrant.getY()*2 + 1);
    g.drawImage(subdivide(image, level+1, newQuadrant, size, prefix), size.getX(), size.getY(), null);
    BufferedImage outputImage = new BufferedImage(size.getX(), size.getY(), BufferedImage.TYPE_INT_RGB);
    outputImage.getGraphics().drawImage(zoomed, 0, 0, size.getX(), size.getY(), null);
    write(outputImage, level, quadrant, prefix);
    return outputImage;    
  }
  
  /*
   * Write image file.
   */
  public void write(BufferedImage image, int level, XYData quadrant, String prefix) {
    StringBuilder outputFile = new StringBuilder();
    outputFile.append(sandbox);
    outputFile.append(File.separator);
    outputFile.append(prefix);
    outputFile.append("-");
    outputFile.append(level);
    outputFile.append("-");
    outputFile.append(quadrant.getX());
    outputFile.append("-");
    outputFile.append(quadrant.getY());
    outputFile.append(".png");
    FileOutputStream fos;
    try {
      fos = new FileOutputStream(outputFile.toString());
      ImageIO.write(image, "PNG", fos);
      fos.close();   
    } catch (IOException e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
  }
  
  public int process(String filename) {
    Pattern p = Pattern.compile("(.*)\\.(.*)");
    Matcher m = p.matcher(filename);
    if(m.matches()) {
      String prefix = m.group(1);
      String fullPath = sandbox + File.separator + filename;
      subdivide(prepare(fullPath), 0, new XYData(0, 0), new XYData(256, 256), prefix);
      return maxLevel;
    }
    return 0;
  }

}

class XYData {
  private int x = 0;
  private int y = 0;
  
  public XYData(int x, int y) {
    this.x=x;
    this.y=y;
  }

  public void set(int x, int y) {
    this.x=x;
    this.y=y;
  }
  
  public int getX() {
    return x;
  }

  public int getY() {
    return y;
  }
  
}
