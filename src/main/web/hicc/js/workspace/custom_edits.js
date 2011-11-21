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

/*
 * toggle the custom period control
 */
function togglePeriodControl(event) {
  var element = Event.element(event);
  value=$F(element.id);
  if (value=='custom') {
    $(element.id+'_custom_block').show();
  } else {
    $(element.id+'_custom_block').hide();
  }
}

/*
 * popup the online help
 */
function popup_help(event) {
  var element = Event.element(event);
  window.open("/hicc/jsp/help.jsp?id="+element.id,
	      "Help",
	      "width=500,height=400");
}

/*
 * For each custom control, you need to provide 3 plugins
 *   * build_custom_edit - build the control's html 
 *   * after_build_custom_edits - setup the control's javascript and notification (optional)
 *   * get_custom_edit_value - return the custom control's value
 */

/*
 * external function to build custom edit component in the widget.
 *
 * param will have all the information to build this edit control
 */
function build_custom_edit(control_id, param) {
  var content='';
  if (param.control == 'period_control') {
    // setup up the period control
    time_options = [['Use Time Widget',''],
		    ['Last 1 Hour','last1hr'],
		    ['Last 2 Hour','last2hr'],
		    ['Last 3 Hour','last3hr'],
		    ['Last 6 Hour','last6hr'],
		    ['Last 12 Hour','last12hr'],
		    ['Last 24 Hour','last24hr'],
		    ['Yesterday','yesterday'],
		    ['Last 7 Days','last7d'],
		    ['Last 30 Days','last30d'],
		    ['Custom Period','custom'],
		    ];

    content+='<select class="formSelect periodControl" id="'+control_id+'" name="'+control_id+'" >';
    for (var j=0;j<time_options.length-1;j++) {
      var option=time_options[j];
      var selected=false;
      var param_value=param.value;

      if (option[1]!=null && param_value==option[1]) {
	selected=true;
      } else {
	if ((option[1]=='custom') && (param_value.startsWith('custom;'))) {
	  selected=true;
	}
      }
      content+='<option '+((selected)?'selected':'')+' value="'+option[1]+'">'+option[0]+'</option>';
    }
    content+='</select>';
    content+='<div id="'+control_id+'_custom_block" style="display:'+(param_value.startsWith('custom;')?'block':'none')+';">';
    start_string='2 days ago';
    end_string='now';
    if (param_value.startsWith('custom;')) {
      param_value_split=param_value.split(";");
      if (param_value_split.length==3) { // custom;start;end
	start_string=param_value_split[1];
	end_string=param_value_split[2];
      }
    }
    // setup the custom date stuff
    content+='<br/>';
    content+='<fieldset>';
    content+='<legend>Custom Period</legend>';
    content+='<table>';
    content+='<tr><td>';
    content+='<label for="start_period">Start Period:</label>';
    content+='</td><td>';
    content+='<input type="edit" name="'+control_id+'_start" id="'+control_id+'_start" value="'+start_string+'"/>';
    content+='</td><td>';
    content+='<a href="#" id="help_edit_start_time" class="help_control">?</a>';
    content+='</td></tr>';
    content+='<tr><td>';
    content+='<label for="end_period">End Period:</label>';
    content+='</td><td>';
    content+='<input type="edit" name="'+control_id+'_end" id="'+control_id+'_end" value="'+end_string+'"/>';
    content+='</td><td>';
    content+='<a href="#" id="help_edit_start_time" class="help_control">?</a>';
    content+='</td></tr>';
    content+='</table>';
    content+='</fieldset>';

    content+='</div>';
  }
  return content;
}

/*
 * function to do post setup for the custom control. For example,
 * hook up the event notification javascript
 */
function after_build_custom_edits(box) {
  if(box.block_obj.parameters!=null) {
	  for (iCount=0;iCount<box.block_obj.parameters.length-1;iCount++) {
	    if (box.block_obj.parameters[iCount].edit!=0) {
	      var param_id='param_'+box.pageid+'_'+box.boxIndex + '_'+iCount;
	      var param=box.block_obj.parameters[iCount];
	      if (param.type=='custom') {

		// setup the period control
		if ("period_control" == param.control) {

		  // hook up the event notification stuff
		  period_control=document.getElementById(param_id);
		  if (period_control != null) {
		    Event.observe(param_id,'change',function(event) {
                      var element = Event.element(event);
                      value=$F(element.id);
                      if (value=='custom') {
                        $(element.id+'_custom_block').show();
                      } else {
                        $(element.id+'_custom_block').hide();
                      }
                    });
		  }

		  // hook up the help
		  help_controls=document.getElementsByClassName("help_control");
		  for (i=0;i<help_controls.length;i++) {
		    help_controls[i].observe('click',popup_help);
		  }
		}
	      }
	    }
	  }
  }
}

/*
 * return the value for the custom control
 */
function get_custom_edit_value(control_id, param) {
  var value='';
  if (param.control == 'period_control') {
    value=$F(control_id);
    if (value == 'custom') {
      value='custom;'+$F(control_id+'_start')+';'+$F(control_id+'_end');
    }
  }
  return value;
}
