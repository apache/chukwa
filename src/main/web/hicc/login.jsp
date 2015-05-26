<%--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  --%>
<html>
<head>
    <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
    <meta http-equiv="Pragma" content="no-cache" />
    <meta http-equiv="Expires" content="0" />
    <link type="text/css" rel="stylesheet" href="css/login.css"/>
    <link href="css/bootstrap.min.css" type="text/css" rel="stylesheet" />
    <link href="css/bootstrap-theme.min.css" type="text/css" rel="stylesheet" />
    <script src="js/jquery.js" type="text/javascript"></script>
    <script src="js/bootstrap.min.js" type="text/javascript"></script>
</head>
<body>

<div class="container">
    <form class="form-signin" name="loginform" action="" method="post">
      <h2 class="form-signin-heading">Sign in to Chukwa</h2>
      <label for="inputEmail" class="sr-only">Username</label>
      <input type="text" class="form-control" name="username" placeholder="Username" required autofocus>
      <label for="inputPassword" class="sr-only">Password:</label>
      <input type="password" class="form-control" name="password" placeholder="Password" required>
      <div class="checkbox">
        <label> 
          <input type="checkbox" name="rememberMe"> Keep me signed in
        </label>
      </div>
      <input type="submit" class="btn btn-lg btn-primary btn-block" name="submit" value="Login">
      <span class="form-signin-footer">
        Copyright (c) 2008-2015 Apache Software Foundation
      </span>
    </form>
</div>

</body>
</html>
