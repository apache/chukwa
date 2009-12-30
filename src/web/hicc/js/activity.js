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

Array.prototype.remove = function(from, to) {
  if (typeof from != "number") return this.remove(this.indexOf(from));
  var rest = this.slice((to || from) + 1 || this.length);
  this.length = from < 0 ? this.length + from : from;
  return this.push.apply(this, rest);
};

jQuery(function ($) {
  var el = document.getElementById("glow");
  var frameRate = 60;
  var speed = 5; // average # of comments per second
  var width = el.width;
  var height = el.height;
  var p = Processing(el);
  p.size(width, height);
  p.colorMode(p.HSB);
  p.noStroke();
  var font = p.loadFont("Helvetica");
  var fontSize = 12;
  p.textFont(font, fontSize);
  var components = [];
  var bottom = 300;
  
  function whiteText(msg, x, y) {
    p.textFont(font, fontSize);
    p.fill(0, 0, 0);
    p.rect(x, y - fontSize, font.width(msg) * fontSize, fontSize + 2);
    p.fill(0, 0, 255);
    p.text(msg, x, y);
  }
  
  var Axes = function Axes() {
    return {
      update: function () {
      },
      draw: function () {
        p.stroke(0, 0, 128);
        p.line(15.5, 0, 15.5, height);
        p.line(15, bottom + height / 10 + 0.5, width, bottom + height / 10 + 0.5);
        for (var y = 0; y < height; y += height / 10) {
          p.line(10, y + 0.5, 15, y + 0.5);
        }
        p.noStroke();
        whiteText("overall activity", 20, fontSize * 1.5);
        whiteText("hosts", width - font.width("hosts") * fontSize * 1.5, bottom + height / 10 + fontSize * 1.2);
      }
    }
  };
  components.push(new Axes());
  
  var Circle = function Circle(height, host, action) {
    var maxAge = 100;
    var letters = "0123456789".split("");
    var x = 0;
    var reverseIP = host.split("").reverse().join("");
    for (var i=0; i<3; i++)
      x += letters.indexOf(reverseIP.charAt(i).toUpperCase()) / Math.pow(letters.length, i + 1);
    
    return {
      age: 0,
      x: x * width * 4 / 5 + width / 5,
      y: bottom - Math.sin(x * Math.PI) * Math.min(height, 90) * 3,
      dx: (Math.random() * width - width / 2) * 0.001,
      dy: (Math.random() * height - height / 2) * 0.001,
      hue: Math.floor(x * 348),
      update: function () {
        this.age++;
        this.x += this.dx;
        this.y += this.dy;
        if (this.age >= maxAge) {
          components.remove(this);
        }
      },
      draw: function () {
        var age = this.age;
        var hue = this.hue + age * 0.1;
        var saturation = age + 155;
        var brightness1 = 192 - age;
        var brightness2 = 128 - age;
        var alpha = 150 - 1.5 * age;
        var r = age * age / 50 + 2 * age + 30; // parabola
        p.fill(hue, saturation, brightness1, alpha);
        p.ellipse(this.x, this.y, r, r);
        p.fill(hue, saturation, brightness2, alpha);
        p.ellipse(this.x, this.y, r * 4 / 5, r * 4 / 5);
        p.fill(0, 0, 255, alpha);
        p.textFont(font, fontSize);
        var label = action + " " + host;
        p.text(label, this.x - Math.floor(font.width(label) * fontSize / 2), this.y);
      }
    };
  };
  
  function parseDate(isoDateString) {
    var d = isoDateString.split(/[: -]/);
    return new Date(Date.UTC(d[0], d[1] - 1, d[2], d[3], d[4], d[5]));
  }
  
  function formatDate(date) {
    var d = date.getDate();
    d = "" + (d < 10 ? "0" : "") + d + " ";
    d += "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(/ /)[date.getMonth()] + " ";
    d += date.getFullYear();
    d = "Sun Mon Tue Wed Thu Fri Sat".split(/ /)[date.getDay()] + " " + d;
    return d;
  }
  
  function formatTime(date) {
    var h = date.getHours();
    var ampm = h >= 12 ? "PM" : "AM";
    var t = "" + (h == 0 ? 12 : (h > 12 ? h - 12 : h)) + ":";
    var m = date.getMinutes();
    t += "" + (m < 10 ? "0" : "") + m + " " + ampm;
    return t;
  }
  
  var Glow = function Glow(comments, pages) {
    var frames = comments.length * frameRate / speed;
    var frame = 0;
    return {
      update: function () {
        frame++;
        while (comments.length > 0) {
          var c = comments.shift();
          if(c.date.getTime() >= last.getTime()) {
            components.push(new Circle(components.length, c.src, c.action));
          }
        }
        if (frame % 5 == 0)
          $("#date").html(formatDate(last) + "<br/>" + formatTime(last));
      },
      draw: function () {
        p.background(0, 15);
      }
    };
  };
  
  var animate = function (frameRate, pause) {
    var runner = function () {
      for (var i=0; i<components.length; i++) {
        components[i].update();
      }
      for (var i=0; i<components.length; i++) {
        components[i].draw();
      }
    };
    var interval = window.setInterval(runner, 1000 / frameRate);
    $("#glow").toggle(function () {
      window.clearInterval(interval);
      p.textFont(font, 24);
      p.fill(0, 0, 255);
      p.text("Paused", width / 2 - font.width("Paused") * 12, 30);
      runner(); // run once more to show message
    }, function () {
      interval = window.setInterval(runner, 1000 / frameRate);
    });
  };

  var last; 
  var refresh = setInterval(function () {
    $.getJSON("/hicc/v1/clienttrace", function (data) {
      var comments = [];
      var pages = {};
      if(data!=null) {
        $.each(data.clientTraceBean, function (i, event) { 
          var current = new Date();
          comments.push({ 
            id: current.getTime(), 
            src: event.src,
            action: event.action,
            date: parseDate(event.date)
            });
          pages[i] = {
            action: event.action,
            date: parseDate(event.date),
            src: event.src
          };
          last = parseDate(event.date);
        }); 
        components.push(new Glow(comments.reverse(), pages));
      }

    });
  }, 15000);
  animate(frameRate, $("#glow"));

});

