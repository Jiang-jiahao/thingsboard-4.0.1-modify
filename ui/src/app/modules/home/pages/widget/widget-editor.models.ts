import { JnksIotEditorCompleter, JnksIotEditorCompletions } from '@shared/models/ace/completion.models';
import {
  widgetContextCompletions,
  widgetContextCompletionsWithSettings
} from '@shared/models/ace/widget-completion.models';
import { serviceCompletions } from '@shared/models/ace/service-completion.models';

const widgetEditorCompletions = (settingsCompletions?: JnksIotEditorCompletions): JnksIotEditorCompletions => {
  return {
    ... {self: {
        description: 'Built-in variable <b>self</b> that is a reference to the widget instance',
        type: '<code>WidgetTypeInstance</code>',
        meta: 'object',
        children: {
          ...{
            onInit: {
              description: 'The first function which is called when widget is ready for initialization.<br>Should be used to prepare widget DOM, process widget settings and initial subscription information.',
              meta: 'function'
            },
            onDataUpdated: {
              description: 'Called when the new data is available from the widget subscription.<br>Latest data can be accessed from ' +
                'the <code>defaultSubscription</code> property of widget context (<code>ctx</code>).',
              meta: 'function'
            },
            onResize: {
              description: 'Called when widget container is resized. Latest <code>width</code> and <code>height</code> can be obtained from widget context (<code>ctx</code>).',
              meta: 'function'
            },
            onEditModeChanged: {
              description: 'Called when dashboard editing mode is changed. Latest mode is handled by <code>isEdit</code> property of widget context (<code>ctx</code>).',
              meta: 'function'
            },
            onMobileModeChanged: {
              description: 'Called when dashboard view width crosses mobile breakpoint. Latest state is handled by <code>isMobile</code> property of widget context (<code>ctx</code>).',
              meta: 'function'
            },
            onDestroy: {
              description: 'Called when widget element is destroyed. Should be used to cleanup all resources if necessary.',
              meta: 'function'
            },
            getSettingsForm: {
              description: 'Optional function returning widget settings form array as alternative to <b>Settings form</b> tab of settings section.',
              meta: 'function',
              return: {
                description: 'An array of widget settings form properties',
                type: 'Array&lt;FormProperty&gt;'
              }
            },
            getDataKeySettingsForm: {
              description: 'Optional function returning particular data key settings form array as alternative to <b>Data key settings form</b> tab of settings section.',
              meta: 'function',
              return: {
                description: 'An array of data key settings form properties',
                type: 'Array&lt;FormProperty&gt;'
              }
            },
            getSettingsSchema: {
              description: '<b>Deprecated</b>. Use getSettingsForm() function.',
              meta: 'function',
              return: {
                description: 'An widget settings schema json',
                type: 'object'
              }
            },
            getDataKeySettingsSchema: {
              description: '<b>Deprecated</b>. Use getDataKeySettingsForm() function.',
              meta: 'function',
              return: {
                description: 'A particular data key settings schema json',
                type: 'object'
              }
            },
            typeParameters: {
              description: 'Returns object describing widget datasource parameters.',
              meta: 'function',
              return: {
                description: 'An object describing widget datasource parameters.',
                type: '<code>WidgetTypeParameters</code>'
              }
            },
            actionSources: {
              description: 'Returns map describing available widget action sources used to define user actions.',
              meta: 'function',
              return: {
                description: 'A map of action sources by action source id.',
                type: '{[actionSourceId: string]: <code>WidgetActionSource</code>}'
              }
            }
          },
          ...widgetContextCompletionsWithSettings(settingsCompletions)
        }
      }}
  }
};

export const widgetEditorCompleter = (settingsCompletions?: JnksIotEditorCompletions): JnksIotEditorCompleter => {
  return new JnksIotEditorCompleter(widgetEditorCompletions(settingsCompletions));
}
