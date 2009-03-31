package org.apache.hadoop.chukwa.hicc;


import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.chukwa.util.XssFilter;

public class Iframe extends HttpServlet {
  public static final long serialVersionUID = 100L;
  private String id;
  private String height = "100%";
  private XssFilter xf = null;

  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    xf = new XssFilter(request);
    if (xf.getParameter("boxId") != null) {
      this.id = xf.getParameter("boxId");
    } else {
      this.id = "0";
    }
    response.setContentType("text/html; chartset=UTF-8//IGNORE");
    response.setHeader("boxId", xf.getParameter("boxId"));
    PrintWriter out = response.getWriter();
    StringBuffer source = new StringBuffer();
    String requestURL = request.getRequestURL().toString().replaceFirst("iframe/", "");
    if(requestURL.indexOf("/hicc")!=-1) {
       requestURL = requestURL.substring(requestURL.indexOf("/hicc"));
    }
    source.append(requestURL);
    source.append("?");
    Enumeration names = request.getParameterNames();
    while (names.hasMoreElements()) {
      String key = xf.filter((String) names.nextElement());
      String[] values = xf.getParameterValues(key);
      for (int i = 0; i < values.length; i++) {
        source.append(key + "=" + values[i] + "&");
      }
      if (key.toLowerCase().intern() == "height".intern()) {
        height = xf.getParameter(key);
      }
    }
    out.println("<html><body><iframe id=\"iframe" + this.id + "\" " + "src=\""
        + source + "\" width=\"100%\" height=\"" + height + "\" "
        + "frameborder=\"0\" style=\"overflow: hidden\"></iframe>");
  }

  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    doGet(request, response);
  }
}
