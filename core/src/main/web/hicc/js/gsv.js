/*
  GSV 1.0, by Michal Migurski <mike-gsv@teczno.com>
  $Id: gsv.js,v 1.6 2005/06/28 03:30:49 migurski Exp $

  Description:
    Generates a draggable and zoomable viewer for images that would be
    otherwise too-large for a browser window, e.g. maps or hi-res
    document scans. Images must be pre-cut into tiles by PowersOfTwo
    Python library.
   	
  Usage:
    For an HTML construct such as this:

        <div class="imageViewer">
            <div class="well"> </div>
            <div class="surface"> </div>
            <p class="status"> </p>
        </div>

    ...pass the DOM node for the top-level DIV, a directory name where
    tile images can be found, and an integer describing the height of
    each image tile to prepareViewer():

        prepareViewer(element, 'tiles', 256);
        
    It is expected that the visual behavior of these nodes is determined
    by a set of CSS rules.
    
    The "well" node is where generated IMG elements are appended. It
    should have the CSS rule "overflow: hidden", to occlude image tiles
    that have scrolled out of view.
    
    The "surface" node is the transparent mouse-responsive layer of the
    image viewer, and should match the well in size.
    
    The "status" node is generally set to "display: none", but can be
    shown when diagnostic information is desired. It's controlled by the
    displayStatus() function here.

  License:
    Copyright (c) 2005 Michal Migurski <mike-gsv@teczno.com>
    
    Redistribution and use in source form, with or without modification,
    are permitted provided that the following conditions are met:
    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.
    2. The name of the author may not be used to endorse or promote products
       derived from this software without specific prior written permission.
    
    THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
    IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
    OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
    IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
    NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
    THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/   

function getEvent(event)
{
    if(event == undefined) {
        return window.event;
    }
    
    return event;
}

function prepareViewer(imageViewer, tileDir, tileSize)
{
    for(var child = imageViewer.firstChild; child; child = child.nextSibling) {
        if(child.className == 'surface') {
            imageViewer.activeSurface = child;
            child.imageViewer = imageViewer;
        
        } else if(child.className == 'well') {
            imageViewer.tileWell = child;
            child.imageViewer = imageViewer;
        
        } else if(child.className == 'status') {
            imageViewer.status = child;
            child.imageViewer = imageViewer;
        
        }
    }
    
    var width = imageViewer.offsetWidth;
    var height = imageViewer.offsetHeight;
    var zoomLevel = -1; // guaranteed at least one increment below, so start at less-than-zero
    var fullSize = tileSize * Math.pow(2, zoomLevel); // full pixel size of the image at this zoom level
    do {
        zoomLevel += 1;
        fullSize *= 2;
    } while(fullSize < Math.max(width, height));

    var center = {'x': ((fullSize - width) / -2), 'y': ((fullSize - height) / -2)}; // top-left pixel of viewer, if it were to be centered in the view window

    imageViewer.style.width = width+'px';
    imageViewer.style.height = height+'px';
    
    var top = 0;
    var left = 0;
    for(var node = imageViewer; node; node = node.offsetParent) {
        top += node.offsetTop;
        left += node.offsetLeft;
    }

    imageViewer.dimensions = {

         // width and height of the viewer in pixels
         'width': width, 'height': height,

         // position of the viewer in the document, from the upper-left corner
         'top': top, 'left': left,

         // location and height of each tile; they're always square
         'tileDir': tileDir, 'tileSize': tileSize,

         // zero or higher; big number == big image, lots of tiles
         'zoomLevel': zoomLevel,

         // initial viewer position
         // defined as window-relative x,y coordinate of upper-left hand corner of complete image
         // usually negative. constant until zoomLevel changes
         'x': center.x, 'y': center.y

         };

    imageViewer.start = {'x': 0, 'y': 0}; // this is reset each time that the mouse is pressed anew
    imageViewer.pressed = false;

    if(document.body.imageViewers == undefined) {
        document.body.imageViewers = [imageViewer];
        document.body.onmouseup = releaseViewer;

    } else {
        document.body.imageViewers.push(imageViewer);
    
    }

    prepareTiles(imageViewer);
}


function prepareTiles(imageViewer)
{
    var activeSurface = imageViewer.activeSurface;
    var tileWell = imageViewer.tileWell;
    var dim = imageViewer.dimensions;

    imageViewer.tiles = [];
    
    var rows = Math.ceil(dim.height / dim.tileSize) + 1;
    var cols = Math.ceil(dim.width / dim.tileSize) + 1;
    
    displayStatus(imageViewer, 'rows: '+rows+', cols: '+cols);
    
    for(var c = 0; c < cols; c += 1) {
        var tileCol = [];
    
        for(var r = 0; r < rows; r += 1) {

            var tile = {'c': c, 'r': r, 'img': document.createElement('img'), 'imageViewer': imageViewer};

            tile.img.className = 'tile';
            tile.img.style.width = dim.tileSize+'px';
            tile.img.style.height = dim.tileSize+'px';
            setTileImage(tile, true);
            
            tileWell.appendChild(tile.img);
            tileCol.push(tile);
        }
        
        imageViewer.tiles.push(tileCol);
    }
    
    activeSurface.onmousedown = pressViewer;
    positionTiles(imageViewer, {'x': 0, 'y': 0}); // x, y should match imageViewer.start x, y
}

function positionTiles(imageViewer, mouse)
{
    var tiles = imageViewer.tiles;
    var dim = imageViewer.dimensions;
    var start = imageViewer.start;
    
    var statusTextLines = [];
    statusTextLines.push('imageViewer.dimensions x,y: '+dim.x+','+dim.y);
    
    for(var c = 0; c < tiles.length; c += 1) {
        for(var r = 0; r < tiles[c].length; r += 1) {

            var tile = tiles[c][r];
            
            // wrappedAround will become true if any tile has to be wrapped around
            var wrappedAround = false;
            
            tile.x = (tile.c * dim.tileSize) + dim.x + (mouse.x - start.x);
            tile.y = (tile.r * dim.tileSize) + dim.y + (mouse.y - start.y);
            
            if(tile.x > dim.width) {
                // tile is too far to the right
                // shift it to the far-left until it's within the viewer window
                do {
                    tile.c -= tiles.length;
                    tile.x = (tile.c * dim.tileSize) + dim.x + (mouse.x - start.x);
                    wrappedAround = true;

                } while(tile.x > dim.width);

            } else {
                // tile may be too far to the right
                // if it is, shift it to the far-right until it's within the viewer window
                while(tile.x < (-1 * dim.tileSize)) {
                    tile.c += tiles.length;
                    tile.x = (tile.c * dim.tileSize) + dim.x + (mouse.x - start.x);
                    wrappedAround = true;

                }
            }
            
            if(tile.y > dim.height) {
                // tile is too far down
                // shift it to the very top until it's within the viewer window
                do {
                    tile.r -= tiles[c].length;
                    tile.y = (tile.r * dim.tileSize) + dim.y + (mouse.y - start.y);
                    wrappedAround = true;

                } while(tile.y > dim.height);

            } else {
                // tile may be too far up
                // if it is, shift it to the very bottom until it's within the viewer window
                while(tile.y < (-1 * dim.tileSize)) {
                    tile.r += tiles[c].length;
                    tile.y = (tile.r * dim.tileSize) + dim.y + (mouse.y - start.y);
                    wrappedAround = true;

                }
            }

            statusTextLines.push('tile '+r+','+c+' at '+tile.c+','+tile.r);
            
            // set the tile image once to *maybe* null, then again to
            // definitely the correct tile. this removes the wraparound
            // artifacts seen over slower connections.
            setTileImage(tile, wrappedAround);
            setTileImage(tile, false);

            tile.img.style.top = tile.y+'px';
            tile.img.style.left = tile.x+'px';
        }
    }
    
    displayStatus(imageViewer, statusTextLines.join('<br>'));
}

function setTileImage(tile, nullOverride)
{
    var dim = tile.imageViewer.dimensions;

    // request a particular image slice
    var src = dim.tileDir+'-'+dim.zoomLevel+'-'+tile.c+'-'+tile.r+'.png';

    // has the image been scrolled too far in any particular direction?
    var left = tile.c < 0;
    var high = tile.r < 0;
    var right = tile.c >= Math.pow(2, tile.imageViewer.dimensions.zoomLevel);
    var low = tile.r >= Math.pow(2, tile.imageViewer.dimensions.zoomLevel);
    var outside = high || left || low || right;

         if(nullOverride)     { src = '/hicc/images/blank.gif';          }

    // note this "outside" clause overrides all those below
    else if(outside)          { src = '/hicc/images/blank.gif';          }

    else if(high && left)     { src = 'null/top-left.png';      }
    else if(low  && left)     { src = 'null/bottom-left.png';   }
    else if(high && right)    { src = 'null/top-right.png';     }
    else if(low  && right)    { src = 'null/bottom-right.png';  }
    else if(high)             { src = 'null/top.png';           }
    else if(right)            { src = 'null/right.png';         }
    else if(low)              { src = 'null/bottom.png';        }
    else if(left)             { src = 'null/left.png';          }

    tile.img.src = src;
}

function moveViewer(event)
{
    var imageViewer = this.imageViewer;
    var ev = getEvent(event);
    var mouse = localizeCoordinates(imageViewer, {'x': ev.clientX, 'y': ev.clientY});

    displayStatus(imageViewer, 'mouse at: '+mouse.x+', '+mouse.y+', '+(imageViewer.tiles.length * imageViewer.tiles[0].length)+' tiles to process');
    positionTiles(imageViewer, {'x': mouse.x, 'y': mouse.y});
}

function localizeCoordinates(imageViewer, client)
{
    var local = {'x': client.x, 'y': client.y};

    for(var node = imageViewer; node; node = node.offsetParent) {
        local.x -= node.offsetLeft;
        local.y -= node.offsetTop;
    }
    
    return local;
}

function pressViewer(event)
{
    var imageViewer = this.imageViewer;
    var dim = imageViewer.dimensions;
    var ev = getEvent(event);
    var mouse = localizeCoordinates(imageViewer, {'x': ev.clientX, 'y': ev.clientY});

    imageViewer.pressed = true;
    imageViewer.tileWell.style.cursor = imageViewer.activeSurface.style.cursor = 'move';
    
    imageViewer.start = {'x': mouse.x, 'y': mouse.y};
    this.onmousemove = moveViewer;

    displayStatus(imageViewer, 'mouse pressed at '+mouse.x+','+mouse.y);
}

function releaseViewer(event)
{
    var ev = getEvent(event);
    
    for(var i = 0; i < document.body.imageViewers.length; i += 1) {
        var imageViewer = document.body.imageViewers[i];
        var mouse = localizeCoordinates(imageViewer, {'x': ev.clientX, 'y': ev.clientY});
        var dim = imageViewer.dimensions;

        if(imageViewer.pressed) {
            imageViewer.activeSurface.onmousemove = null;
            imageViewer.tileWell.style.cursor = imageViewer.activeSurface.style.cursor = 'default';
            imageViewer.pressed = false;

            dim.x += (mouse.x - imageViewer.start.x);
            dim.y += (mouse.y - imageViewer.start.y);
        }

        displayStatus(imageViewer, 'mouse dragged from '+imageViewer.start.x+', '+imageViewer.start.y+' to '+mouse.x+','+mouse.y+'. image: '+dim.x+','+dim.y);
    }
}

function displayStatus(imageViewer, message)
{
    imageViewer.status.innerHTML = message;
}

function dumpInfo(imageViewer)
{
    var dim = imageViewer.dimensions;
    var tiles = imageViewer.tiles;

    var statusTextLines = ['imageViewer '+(i + 1), 'current window position: '+dim.x+','+dim.y+'.', '----'];

    for(var c = 0; c < tiles.length; c += 1) {
        for(var r = 0; r < tiles[c].length; r += 1) {
            statusTextLines.push('image ('+c+','+r+') has tile ('+dim.zoomLevel+','+tiles[c][r].c+','+tiles[c][r].r+')');
        }
    }
    
    alert(statusTextLines.join("\n"));
}

function dumpAllInfo()
{
    for(var i = 0; i < document.body.imageViewers.length; i += 1) {
        dumpInfo(document.body.imageViewers[i]);
    }
}

function zoomImage(imageViewer, mouse, direction, max)
{
    var dim = imageViewer.dimensions;
    
    if(mouse == undefined) {
        var mouse = {'x': dim.width / 2, 'y': dim.height / 2};
    }

    var pos = {'before': {'x': 0, 'y': 0}};

    // pixel position within the image is a function of the
    // upper-left-hand corner of the viewe in the page (pos.before),
    // the click position (event), and the image position within
    // the viewer (dim).
    pos.before.x = (mouse.x - pos.before.x) - dim.x;
    pos.before.y = (mouse.y - pos.before.y) - dim.y;
    pos.before.width = pos.before.height = Math.pow(2, dim.zoomLevel) * dim.tileSize;
    
    var statusMessage = ['at current zoom level, image is '+pos.before.width+' pixels wide',
                         '...mouse position is now '+pos.before.x+','+pos.before.y+' in the full image at zoom '+dim.zoomLevel,
                         '...with the corner at '+dim.x+','+dim.y];

    if(dim.zoomLevel + direction >= 0 && dim.zoomLevel + direction <= max) {
        pos.after = {'width': (pos.before.width * Math.pow(2, direction)), 'height': (pos.before.height * Math.pow(2, direction))};
        statusMessage.push('at zoom level '+(dim.zoomLevel + direction)+', image is '+pos.after.width+' pixels wide');

        pos.after.x = pos.before.x * Math.pow(2, direction);
        pos.after.y = pos.before.y * Math.pow(2, direction);
        statusMessage.push('...so the current mouse position would be '+pos.after.x+','+pos.after.y);

        pos.after.left = mouse.x - pos.after.x;
        pos.after.top = mouse.y - pos.after.y;
        statusMessage.push('...with the corner at '+pos.after.left+','+pos.after.top);
        
        dim.x = pos.after.left;
        dim.y = pos.after.top;
        dim.zoomLevel += direction;
        
        imageViewer.start = mouse;
        positionTiles(imageViewer, mouse);
    }

    displayStatus(imageViewer, statusMessage.join('<br>'));
}

function zoomImageUp(imageViewer, mouse, max)
{
    zoomImage(imageViewer, mouse, 1, max);
}

function zoomImageDown(imageViewer, mouse, max)
{
    zoomImage(imageViewer, mouse, -1, max);
}
