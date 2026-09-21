#### Node relations query function

<div class="divider"></div>
<br/>

*function (nodeCtx): `EntityRelationsQuery` | 'default'*

A JavaScript function used to compute child nodes relations query for current node.

**Parameters:**

<ul>
  <li><b>widgetCtx:</b> <code><code>WidgetContext</code></code> - A reference to <code>WidgetContext</code> that has all necessary API 
     and data used by widget instance.
  </li>
  <li><b>nodeCtx:</b> <code><code>HierarchyNodeContext</code></code> - An 
            <code>HierarchyNodeContext</code> object
            containing <code>entity</code> field holding basic entity properties <br> (ex. <code>id</code>, <code>name</code>, <code>label</code>) and <code>data</code> field holding other entity attributes/timeseries declared in widget datasource configuration.
   </li>
</ul>

**Returns:**

Should return `EntityRelationsQuery` for current node used to fetch entity children.<br>
Function can return `default` string value. In this case default relations query will be used.

<div class="divider"></div>

##### Examples

* Fetch child entities having relations of type `Contains` from the current entity:

```javascript
var entity = nodeCtx.entity;
var query = {
  parameters: {
    rootId: entity.id.id,
    rootType: entity.id.entityType,
    direction: "FROM",
    maxLevel: 1
  },
  filters: [{
    relationType: "Contains",
    entityTypes: []
  }]
};
return query;
{:copy-code}
```

<br>
<br>
