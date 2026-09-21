package com.jnks.iot.server.msa.ui.pages;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import com.jnks.iot.server.msa.ui.base.AbstractBasePage;

public class OpenRuleChainPageElements extends AbstractBasePage {
    public OpenRuleChainPageElements(WebDriver driver) {
        super(driver);
    }

    private static final String DONE_BTN = "//mat-icon[contains(text(),'done')]/parent::button";
    private static final String INPUT_NODE = "//div[@class='jnks-iot-rule-node jnks-iot-input-type']";
    private static final String HEAD_RULE_CHAIN_NAME = "//div[@class='jnks-iot-breadcrumb']/span[2]";

    public WebElement inputNode() {
        return waitUntilVisibilityOfElementLocated(INPUT_NODE);
    }

    public WebElement headRuleChainName() {
        return waitUntilVisibilityOfElementLocated(HEAD_RULE_CHAIN_NAME);
    }

    public WebElement doneBtn() {
        return waitUntilElementToBeClickable(DONE_BTN);
    }

}
