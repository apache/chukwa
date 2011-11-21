<%
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
%>
<%@ page import = "org.apache.hadoop.chukwa.util.XssFilter" %>
<%@ page import = "java.util.regex.Matcher" %>
<%@ page import = "java.util.regex.Pattern" %>
<%
  Pattern p = Pattern.compile("(.*)\\.(.*)");
  Matcher m = p.matcher(request.getAttribute("image-viewer").toString());
  String filename = "";
  if(m.matches()) {
    filename = m.group(1);
  }
  int maxLevel = Integer.parseInt(request.getAttribute("maxLevel").toString());
%>
<html>
  <head>
    <style type="text/css" title="text/css">
    <!--
        body
        {
            color: black;
            background-color: white;
            font-family: helvetica, arial, sans-serif;
        }
        
        .imageViewer
        {
            position: relative;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
        }
        
        .imageViewer .well, .imageViewer .surface
        {
            margin: 0;
            padding: 0;
            width: 100%;
            height: 100%;
            position: absolute;
            top: 0px;
            left: 0px;
            cursor: default;
            border: 1px solid black;
        }
        
        .imageViewer .well
        {
            background-color: gray;
            background-image: url("/hicc/images/blank.gif");
            overflow: hidden;
        }
        
        .imageViewer .surface        
        {
            background-color: transparent;
            background-image: url("/hicc/images/blank.gif");
            background-repeat: no-repeat;
            background-position: center center;
        }
        
        .imageViewer .status
        {
            margin: 0;
            padding: 0;
            position: absolute;
            top: 480px;
            left: 0px;
            display: none;
        }
        
            .imageViewer .well .tile
            {
                border: 0;
                margin: 0;
                padding: 0;
                position: absolute;
                top: 0px;
                left: 0px;
                display: block;
            }
            
        .imageViewer .zoom        
        {
            background-color: white;
            position: absolute;
            top: 0px;
            right: 0px;
            width: 32px;
            height: 20px;
            margin: 0;
            padding: 0 0 0 4px;
            font-size: 20px;
            line-height: 20px;
            font-weight: bold;
            border-left: 1px solid black;
            border-top: 1px solid black;
        }
        
            .imageViewer .zoom a
            {
                text-decoration: none;
                color: black;
            }
        
            .imageViewer .zoom .dump
            {
                font-size: 16px;
            }
        
        h1, .description
        {
            margin-left: 100px;
            width: 400px;
        }
        
        h1
        {
            margin-top: 40px;
        }
        
            h1 em
            {
                font-size: 50%;
                color: gray;
            }
        
    -->
    </style>

    <script src="/hicc/js/gsv.js" type="text/javascript"></script>
    <script src="/hicc/js/behaviour.js" type="text/javascript"></script>

    <script type="text/javascript">
    <!--

        Behaviour.register({
            '.imageViewer' : function(el) {
                prepareViewer(el, '/sandbox/<%= filename %>', 256);
            },
            '.imageViewer .zoom .up' : function(el) {
                el.onclick = function() {
                    zoomImageUp(el.parentNode.parentNode, undefined, <%= maxLevel %>);
                    return false;
                }
            },
            '.imageViewer .zoom .down' : function(el) {
                el.onclick = function() {
                    zoomImageDown(el.parentNode.parentNode, undefined, <%= maxLevel %>);
                    return false;
                }
            },
            '.imageViewer .zoom .dump' : function(el) {
                el.onclick = function() {
                    dumpInfo(el.parentNode.parentNode);
                    return false;
                }
            }
        });
    
    //-->
    </script>
  </head>
<body>
    <div class="imageViewer">
        <div class="well"> </div>
        <div class="surface"> </div>
        <p class="status"> </p>
        <p class="zoom">

            <a class="up" href="#">+</a>
            <a class="down" href="#">-</a>
            <!-- a class="dump" href="#">?</a -->
        </p>
    </div>
</body>
</html>
