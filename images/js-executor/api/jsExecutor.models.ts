
export interface JnksIotMessage {
    scriptHash: string;
}

export interface RemoteJsRequest {
    compileRequest?: JsCompileRequest;
    invokeRequest?: JsInvokeRequest;
    releaseRequest?: JsReleaseRequest;
}

export interface JsReleaseRequest extends JnksIotMessage {
    functionName: string;
}

export interface JsInvokeRequest extends JnksIotMessage {
    functionName: string;
    scriptBody: string;
    timeout: number;
    args: string[];
}

export interface JsCompileRequest extends JnksIotMessage {
    functionName: string;
    scriptBody: string;
}


export interface  JsReleaseResponse extends JnksIotMessage {
    success: boolean;
}

export interface JsCompileResponse extends JnksIotMessage {
    success: boolean;
    errorCode?: number;
    errorDetails?: string;
}

export interface JsInvokeResponse {
    success: boolean;
    result?: string;
    errorCode?: number;
    errorDetails?: string;
}

export interface RemoteJsResponse {
    requestIdMSB: string;
    requestIdLSB: string;
    compileResponse?: JsCompileResponse;
    invokeResponse?: JsInvokeResponse;
    releaseResponse?: JsReleaseResponse;
}
