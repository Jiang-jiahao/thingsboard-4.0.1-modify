import { Component } from '@angular/core';
import { UntypedFormBuilder, UntypedFormGroup } from '@angular/forms';
import { RuleNodeConfiguration, RuleNodeConfigurationComponent } from '@shared/models/rule-node.models';

@Component({
  selector: 'tb-action-node-rpc-reply-config',
  templateUrl: './rpc-reply-config.component.html',
  styleUrls: []
})
export class RpcReplyConfigComponent extends RuleNodeConfigurationComponent {

  rpcReplyConfigForm: UntypedFormGroup;

  constructor(private fb: UntypedFormBuilder) {
    super();
  }

  protected configForm(): UntypedFormGroup {
    return this.rpcReplyConfigForm;
  }

  protected onConfigurationSet(configuration: RuleNodeConfiguration) {
    this.rpcReplyConfigForm = this.fb.group({
      serviceIdMetaDataAttribute: [configuration ? configuration.serviceIdMetaDataAttribute : null, []],
      sessionIdMetaDataAttribute: [configuration ? configuration.sessionIdMetaDataAttribute : null, []],
      requestIdMetaDataAttribute: [configuration ? configuration.requestIdMetaDataAttribute : null, []]
    });
  }
}
