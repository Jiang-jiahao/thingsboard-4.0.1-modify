#### Row style function

<div class="divider"></div>
<br/>

*function (alarm, ctx): {[key: string]: string}*

A JavaScript function used to compute alarm row style depending on alarm value.

**Parameters:**

<ul>
  <li><b>alarm:</b> <code><code>AlarmDataInfo</code></code> - An 
            <code>AlarmDataInfo</code> object
            presenting basic alarm properties (ex. <code>type</code>, <code>severity</code>, <code>originator</code>, etc.) and <br> provides access to other alarm or originator entity fields/attributes/timeseries declared in widget datasource configuration.
  </li>
  <li><b>ctx:</b> <code><code>WidgetContext</code></code> - A reference to <code>WidgetContext</code> that has all necessary API 
     and data used by widget instance.
  </li>
</ul>

**Returns:**

Should return key/value object presenting style attributes.

<div class="divider"></div>

##### Examples

* Set row background color depending on alarm severity:

```javascript
var severity = alarm.severity;
var color = '#fff';
switch (severity) {
  case 'CRITICAL':
    color = 'red';
    break;
  case 'MAJOR':
    color = 'orange';
    break;
  case 'MINOR':
    color = '#ffca3d';
    break;
  case 'WARNING':
    color = '#abab00';
    break;
  case 'INDETERMINATE':
    color = 'green';
    break;
}
return {
  backgroundColor: color
};
{:copy-code}
```

<br>
<br>
