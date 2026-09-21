#### Cell style function

<div class="divider"></div>
<br/>

*function (value, alarm, ctx): {[key: string]: string}*

A JavaScript function used to compute alarm cell style depending on alarm field value.

**Parameters:**

<ul>
  <li><b>value:</b> <code>any</code> - An alarm field value displayed in the cell.
  </li>
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

* Set color depending on alarm severity:

```javascript
var severity = value;
var color = 'black';
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
  fontWeight: 'bold',
  color: color
};
{:copy-code}
```

<br>
<br>
