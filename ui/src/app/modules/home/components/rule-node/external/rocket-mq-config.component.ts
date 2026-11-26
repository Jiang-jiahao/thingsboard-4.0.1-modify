import { Component } from '@angular/core';
import { UntypedFormBuilder, UntypedFormGroup, Validators } from '@angular/forms';
import { RuleNodeConfiguration, RuleNodeConfigurationComponent } from '@shared/models/rule-node.models';

@Component({
  selector: 'jnks-iot-external-node-rocket-mq-config',
  templateUrl: './rocket-mq-config.component.html',
  styleUrls: []
})
export class RocketMqConfigComponent extends RuleNodeConfigurationComponent {

  rocketMqConfigForm: UntypedFormGroup;

  sendModes = ['ASYNC', 'SYNC', 'ONEWAY'];

  constructor(private fb: UntypedFormBuilder) {
    super();
  }

  protected configForm(): UntypedFormGroup {
    return this.rocketMqConfigForm;
  }

  protected onConfigurationSet(configuration: RuleNodeConfiguration) {
    this.rocketMqConfigForm = this.fb.group({
      topicPattern: [configuration ? configuration.topicPattern : null, [Validators.required]],
      keyPattern: [configuration ? configuration.keyPattern : null],
      tagPattern: [configuration ? configuration.tagPattern : null],
      nameServerAddress: [configuration ? configuration.nameServerAddress : null, [Validators.required]],
      producerGroup: [configuration ? configuration.producerGroup : null, [Validators.required]],
      sendMode: [configuration ? configuration.sendMode : null, [Validators.required]],
      sendMsgTimeout: [configuration ? configuration.sendMsgTimeout : null, [Validators.min(1)]],
      retryTimes: [configuration ? configuration.retryTimes : null, [Validators.min(0)]],
      namespace: [configuration ? configuration.namespace : null],
      accessKey: [configuration ? configuration.accessKey : null],
      secretKey: [configuration ? configuration.secretKey : null],
      addMetadataKeyValuesAsProperties: [configuration ? configuration.addMetadataKeyValuesAsProperties : false, []]
    });
  }

}
