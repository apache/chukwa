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
package org.apache.hadoop.chukwa.datacollection.adaptor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import junit.framework.TestCase;

public class TestSocketAdaptor extends TestCase {
  public void testBindRetry() {
    int port = 9181;
    int delay = 120000;
    ServerSocket sock1 = null;
    ServerSocket sock2 = null;
    SocketAdaptor adaptor = new SocketAdaptor();
    SocketAdaptor.Dispatcher disp = adaptor.new Dispatcher(port);
    // test failure case
    try {
      sock1 = new ServerSocket();
      sock1.setReuseAddress(true);
      sock1.bind(new InetSocketAddress(port));
      System.out.println("Bound to " + port);
      assertTrue(sock1.isBound());
    } catch (IOException e) {
      fail("IOException binding to " + port);
    }
    // now try binding to the same port through SocketAdaptor
    // making sure we retry until the specified time of 120s
    long startTime = System.currentTimeMillis();
    try {
      sock2 = new ServerSocket();
      sock2.setReuseAddress(true);
      disp.bindWithExponentialBackoff(sock2, port, delay);
      // we should not reach this statement
      assertTrue(!sock2.isBound());
    } catch (IOException ioe) {
      long retryInterval = System.currentTimeMillis() - startTime;
      System.out.println("Retried number of milliseconds :" + retryInterval);
      if (retryInterval < delay) {
        fail("SocketAdaptor did not retry bind for milliseconds:" + delay);
      }
    } finally {
      try {
        if (sock1 != null)
          sock1.close();
      } catch (IOException ignore) {
      }
    }

    // test successful case
    startTime = System.currentTimeMillis();
    try {
      disp.bindWithExponentialBackoff(sock2, port, delay);
    } catch (IOException ioe) {
      fail("IOException when trying to bind for the second time");
    }
    assertTrue(sock2.isBound());
    System.out.println("Binding successful in milliseconds:"
        + (System.currentTimeMillis() - startTime));
    if (sock2 != null) {
      try {
        sock2.close();
      } catch (IOException ignore) {
      }
    }
  }
}
