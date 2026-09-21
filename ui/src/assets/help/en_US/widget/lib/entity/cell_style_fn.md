#### Cell style function

<div class="divider"></div>
<br/>

*function (value, entity, ctx): {[key: string]: string}*

A JavaScript function used to compute entity cell style depending on entity field value.

**Parameters:**

<ul>
  <li><b>value:</b> <code>any</code> - An entity field value displayed in the cell.
  </li>
  <li><b>entity:</b> <code><code>EntityData</code></code> - An 
            <code>EntityData</code> object
            presenting basic entity properties (ex. <code>id</code>, <code>entityName</code>) and <br> provides access to other entity attributes/timeseries declared in widget datasource configuration.
  </li>
  <li><b>ctx:</b> <code><code>WidgetContext</code></code> - A reference to <code>WidgetContext</code> that has all necessary API 
     and data used by widget instance.
  </li>
</ul>

**Returns:**

Should return key/value object presenting style attributes.

<div class="divider"></div>

##### Examples

* Set color and font-weight table cell content:

```javascript
return {
  color:'rgb(0, 132, 214)',
  fontWeight: 600
}
{:copy-code}
```

* Set color depending on device temperature value:

```javascript
var temperature = value;
var color = 'black';
if (temperature) {
    if (temperature > 25) {
      color = 'red';
    } else {
      color = 'green';
    }
}
return {
  fontWeight: 'bold',
  color: color
};
{:copy-code}
```

<br>
<br>
