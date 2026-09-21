#### Action sources object

<div class="divider"></div>
<br/>

Map describing available widget action sources (`WidgetActionSource`) to which user actions can be assigned. It has the following structure:

```javascript
   return {
        'headerButton': { // Action source Id (unique action source identificator)
           name: 'widget-action.header-button', // Display name of action source, used in widget settings ('Actions' tab).
           value: 'headerButton', // Action source Id
           multiple: true // Boolean property indicating if this action source supports multiple action definitions
                          // (for ex. multiple buttons in one cell, or only one action can by assigned on table row click.)
        }
    };   
```
