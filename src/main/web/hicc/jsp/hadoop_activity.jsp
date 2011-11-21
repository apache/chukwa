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
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">

<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
  <title>Activity</title>
  <script src="/hicc/js/processing.js" type="text/javascript" charset="utf-8"></script>
  <script src="/hicc/js/jquery-1.3.2.min.js" type="text/javascript" charset="utf-8"></script>
  <script src="/hicc/js/activity.js" type="text/javascript" charset="utf-8"></script>

  <style type="text/css" media="screen">
    body {
        background: #000;
        color: #aaa;
        font-family: Helvetica, sans-serif;
    }
    h1 {
        font-size: 2.5em;
        margin: 0;
        margin-top: .5em;
        text-align: center;
        color: #eee;
        font-weight: bold;
    }
    #glow {
      margin: 1em auto;
      width: 1000px;
      display: block;
    }
    #date {
      text-align: center;
      font-size: 1.5em;
    }
    #footer {
        position: absolute;
        bottom: 1em;
        right: 1em;
        font-size: 12px;
    }
    #footer a {
        color: #eee;
    }
  </style>
</head>
<body>
<h1>Hadoop activity</h1>
<canvas id="glow" width="1000" height="500"></canvas>
<p id="date"></p>
</body>
</html>

