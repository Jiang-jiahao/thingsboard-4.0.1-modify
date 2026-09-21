#### Row style function

<div class="divider"></div>
<br/>

*function (entity, ctx): {[key: string]: string}*

A JavaScript function used to compute entity row style depending on entity value.

**Parameters:**

<ul>
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

* Set color and font-weight table row:

```javascript
return {
  color:'rgb(0, 132, 214)',
  fontWeight: 600
}
{:copy-code}
```

* Set row background color depending on device type:

```javascript
var deviceType = entity.Type;
var color = '#fff';
switch (deviceType) {
  case 'thermostat':
    color = 'orange';
    break;
  case 'default':
    color = '#abab00';
    break;
}
return {
  backgroundColor: color
};
{:copy-code}
```

<br>
<br>
