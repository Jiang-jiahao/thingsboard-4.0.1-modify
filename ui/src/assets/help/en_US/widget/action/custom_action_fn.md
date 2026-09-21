#### Custom action function

<div class="divider"></div>
<br/>

*function ($event, widgetContext, entityId, entityName, additionalParams, entityLabel): void*

A JavaScript function performing custom action.

**Parameters:**

<ul>
  {% include widget/action/custom_action_args %}
</ul>

<div class="divider"></div>

##### Examples

<br>

<div style="padding-left: 32px;"
     jnks-iot-help-popup="widget/action/examples_custom_action/custom_action_display_alert"
     jnks-iot-help-popup-placement="top"
     [jnks-iot-help-popup-style]="{maxHeight: '50vh', maxWidth: '50vw'}"
     trigger-style="font-size: 16px;"
     trigger-text="Display alert dialog with entity information">
</div>

<br>

<div style="padding-left: 32px;"
     jnks-iot-help-popup="widget/action/examples_custom_action/custom_action_delete_device_confirm"
     jnks-iot-help-popup-placement="top"
     [jnks-iot-help-popup-style]="{maxHeight: '50vh', maxWidth: '50vw'}"
     trigger-style="font-size: 16px;"
     trigger-text="Delete device after confirmation">
</div>

<br>

<div style="padding-left: 32px;"
     jnks-iot-help-popup="widget/action/examples_custom_action/custom_action_return_previous_state"
     jnks-iot-help-popup-placement="top"
     [jnks-iot-help-popup-style]="{maxHeight: '50vh', maxWidth: '50vw'}"
     trigger-style="font-size: 16px;"
     trigger-text="Return to the previous state">
</div>

<br>

<div style="padding-left: 32px;"
     jnks-iot-help-popup="widget/action/examples_custom_action/custom_action_open_state_save_parameters"
     jnks-iot-help-popup-placement="top"
     [jnks-iot-help-popup-style]="{maxHeight: '50vh', maxWidth: '50vw'}"
     trigger-style="font-size: 16px;"
     trigger-text="Open state conditionally with saving particular state parameters">
</div>

<br>

<div style="padding-left: 32px;"
     jnks-iot-help-popup="widget/action/examples_custom_action/custom_action_back_first_and_open_state"
     jnks-iot-help-popup-placement="top"
     [jnks-iot-help-popup-style]="{maxHeight: '50vh', maxWidth: '50vw'}"
     trigger-style="font-size: 16px;"
     trigger-text="Go back to the first state, after this go to the target state">
</div>

<br>

<div style="padding-left: 32px;"
     jnks-iot-help-popup="widget/action/examples_custom_action/custom_action_copy_access_token"
     jnks-iot-help-popup-placement="top"
     [jnks-iot-help-popup-style]="{maxHeight: '50vh', maxWidth: '50vw'}"
     trigger-style="font-size: 16px;"
     trigger-text="Copy device access token to buffer">
</div>

<br>
<br>
