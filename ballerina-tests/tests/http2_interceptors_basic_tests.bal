// Copyright (c) 2022 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/http;
import ballerina/test;

int http2InterceptorBasicTestsPort1 = getHttp2Port(interceptorBasicTestsPort1);

final http:Client http2InterceptorsBasicTestsClientEP1 = check new("http://localhost:" + http2InterceptorBasicTestsPort1.toString(), 
    httpVersion = "2.0", http2Settings = { http2PriorKnowledge: true });

listener http:Listener http2InterceptorsBasicTestsServerEP1 = new(http2InterceptorBasicTestsPort1);

@http:ServiceConfig {
    interceptors : [new DefaultRequestInterceptor(), new LastRequestInterceptor(), new DefaultRequestErrorInterceptor()]
}
service /defaultRequestInterceptor on http2InterceptorsBasicTestsServerEP1 {

    resource function 'default .(http:Caller caller, http:Request req) returns error? {
        http:Response res = new();
        res.setHeader("default-request-interceptor", check req.getHeader("default-request-interceptor"));
        res.setHeader("last-request-interceptor", check req.getHeader("last-request-interceptor"));
        string lastInterceptor = req.hasHeader("default-request-error-interceptor") ? "default-request-error-interceptor" : check req.getHeader("last-interceptor");
        res.setHeader("last-interceptor", lastInterceptor);
        check caller->respond(res);
    }
}

@test:Config{}
function tesHttp2DefaultRequestInterceptor() returns error? {
    http:Response res = check http2InterceptorsBasicTestsClientEP1->get("/defaultRequestInterceptor");
    assertHeaderValue(check res.getHeader("last-interceptor"), "default-request-interceptor");
    assertHeaderValue(check res.getHeader("default-request-interceptor"), "true");
    assertHeaderValue(check res.getHeader("last-request-interceptor"), "true");

    res = check http2InterceptorsBasicTestsClientEP1->post("/defaultRequestInterceptor", "testMessage");
    assertHeaderValue(check res.getHeader("last-interceptor"), "default-request-interceptor");
    assertHeaderValue(check res.getHeader("default-request-interceptor"), "true");
    assertHeaderValue(check res.getHeader("last-request-interceptor"), "true");
}

@http:ServiceConfig {
    interceptors : [new LastResponseInterceptor(), new DefaultResponseErrorInterceptor(), new DefaultResponseInterceptor()]
}
service /defaultResponseInterceptor on http2InterceptorsBasicTestsServerEP1 {

    resource function 'default .(http:Request req) returns string {
        string|error payload = req.getTextPayload();
        if payload is error {
            return "Greetings!";
        } else {
            return payload;
        }
    }
}

@test:Config{}
function tesHttp2tDefaultResponseInterceptor() returns error? {
    http:Response res = check http2InterceptorsBasicTestsClientEP1->get("/defaultResponseInterceptor");
    assertTextPayload(res.getTextPayload(), "Greetings!");
    assertHeaderValue(check res.getHeader("last-interceptor"), "default-response-interceptor");
    assertHeaderValue(check res.getHeader("default-response-interceptor"), "true");
    assertHeaderValue(check res.getHeader("last-response-interceptor"), "true");

    res = check http2InterceptorsBasicTestsClientEP1->post("/defaultResponseInterceptor", "testMessage");
    assertTextPayload(res.getTextPayload(), "testMessage");
    assertHeaderValue(check res.getHeader("last-interceptor"), "default-response-interceptor");
    assertHeaderValue(check res.getHeader("default-response-interceptor"), "true");
    assertHeaderValue(check res.getHeader("last-response-interceptor"), "true");
}

@http:ServiceConfig {
    interceptors : [
        new LastResponseInterceptor(), new DefaultRequestInterceptor(), new DefaultResponseInterceptor(), 
        new RequestInterceptorReturnsError(), new LastRequestInterceptor()
    ]
}
service /requestInterceptorReturnsError on http2InterceptorsBasicTestsServerEP1 {

    resource function 'default .() returns string {
        return "Response from resource - test";
    }
}

@test:Config{}
function tesHttp2RequestInterceptorReturnsError() returns error? {
    http:Response res = check http2InterceptorsBasicTestsClientEP1->get("/requestInterceptorReturnsError");
    test:assertEquals(res.statusCode, 500);
    assertTextPayload(check res.getTextPayload(), "Request interceptor returns an error");
}

int http2ResponseInterceptorReturnsErrorTestPort = getHttp2Port(responseInterceptorReturnsErrorTestPort);

final http:Client http2ResponseInterceptorReturnsErrorTestClientEP = check new("http://localhost:" + http2ResponseInterceptorReturnsErrorTestPort.toString(), 
    httpVersion = "2.0", http2Settings = { http2PriorKnowledge: true });

listener http:Listener http2ResponseInterceptorReturnsErrorTestServerEP = new(http2ResponseInterceptorReturnsErrorTestPort, config = {
    interceptors : [new LastResponseInterceptor(), new ResponseInterceptorReturnsError(), new DefaultResponseInterceptor()]
});

service / on http2ResponseInterceptorReturnsErrorTestServerEP {

    resource function 'default .() returns string {
        return "Response from resource - test";
    }
}

@test:Config{}
function tesHttp2ResponseInterceptorReturnsError() returns error? {
    http:Response res = check http2ResponseInterceptorReturnsErrorTestClientEP->get("/");
    test:assertEquals(res.statusCode, 500);
    assertTextPayload(check res.getTextPayload(), "Response interceptor returns an error");
}

int http2InterceptorBasicTestsPort2 = getHttp2Port(interceptorBasicTestsPort2);

final http:Client http2InterceptorsBasicTestsClientEP2 = check new("http://localhost:" + http2InterceptorBasicTestsPort2.toString(), 
    httpVersion = "2.0", http2Settings = { http2PriorKnowledge: true });

listener http:Listener http2InterceptorsBasicTestsServerEP2 = new(http2InterceptorBasicTestsPort2);

@http:ServiceConfig {
    interceptors : [
        new RequestInterceptorReturnsError(), new DefaultRequestInterceptor(), new DefaultRequestErrorInterceptor(), 
        new LastRequestInterceptor()
    ]
}
service /requestErrorInterceptor1 on http2InterceptorsBasicTestsServerEP2 {

    resource function 'default .(http:Caller caller, http:Request req) returns error? {
        http:Response res = new();
        res.setTextPayload(check req.getTextPayload());
        string default_interceptor_header = req.hasHeader("default-request-interceptor") ? "true" : "false";
        res.setHeader("last-interceptor", check req.getHeader("last-interceptor"));
        res.setHeader("default-request-interceptor", default_interceptor_header);
        res.setHeader("last-request-interceptor", check req.getHeader("last-request-interceptor"));
        res.setHeader("request-interceptor-error", check req.getHeader("request-interceptor-error"));
        res.setHeader("default-request-error-interceptor", check req.getHeader("default-request-error-interceptor"));
        check caller->respond(res);
    }
}

@http:ServiceConfig {
    interceptors : [
        new RequestInterceptorReturnsError(), new RequestErrorInterceptorReturnsError(), new DefaultRequestInterceptor(), 
        new DefaultRequestErrorInterceptor(), new LastRequestInterceptor()
    ]
}
service /requestErrorInterceptor2 on http2InterceptorsBasicTestsServerEP2 {

    resource function 'default .(http:Caller caller, http:Request req) returns error? {
        http:Response res = new();
        res.setTextPayload(check req.getTextPayload());
        string default_interceptor_header = req.hasHeader("default-request-interceptor") ? "true" : "false";
        res.setHeader("last-interceptor", check req.getHeader("last-interceptor"));
        res.setHeader("default-request-interceptor", default_interceptor_header);
        res.setHeader("last-request-interceptor", check req.getHeader("last-request-interceptor"));
        res.setHeader("request-interceptor-error", check req.getHeader("request-interceptor-error"));
        res.setHeader("request-error-interceptor-error", check req.getHeader("request-error-interceptor-error"));
        res.setHeader("default-request-error-interceptor", check req.getHeader("default-request-error-interceptor"));
        check caller->respond(res);
    }
}

@test:Config{}
function tesHttp2RequestErrorInterceptor1() returns error? {
    http:Response res = check http2InterceptorsBasicTestsClientEP2->get("/requestErrorInterceptor1");
    test:assertEquals(res.statusCode, 200);
    assertTextPayload(check res.getTextPayload(), "Request interceptor returns an error");
    assertHeaderValue(check res.getHeader("last-interceptor"), "default-request-error-interceptor");
    assertHeaderValue(check res.getHeader("default-request-interceptor"), "false");
    assertHeaderValue(check res.getHeader("last-request-interceptor"), "true");
    assertHeaderValue(check res.getHeader("request-interceptor-error"), "true");
    assertHeaderValue(check res.getHeader("default-request-error-interceptor"), "true");
}

@test:Config{}
function tesHttp2RequestErrorInterceptor2() returns error? {
    http:Response res = check http2InterceptorsBasicTestsClientEP2->get("/requestErrorInterceptor2");
    test:assertEquals(res.statusCode, 200);
    assertTextPayload(check res.getTextPayload(), "Request error interceptor returns an error");
    assertHeaderValue(check res.getHeader("last-interceptor"), "default-request-error-interceptor");
    assertHeaderValue(check res.getHeader("default-request-interceptor"), "false");
    assertHeaderValue(check res.getHeader("last-request-interceptor"), "true");
    assertHeaderValue(check res.getHeader("request-interceptor-error"), "true");
    assertHeaderValue(check res.getHeader("request-error-interceptor-error"), "true");
    assertHeaderValue(check res.getHeader("default-request-error-interceptor"), "true");
}

@http:ServiceConfig{
    interceptors: [
        new LastResponseInterceptor(), new DefaultResponseErrorInterceptor(), new DefaultResponseInterceptor(),
        new ResponseInterceptorReturnsError()
    ]
}
service /responseErrorInterceptor1 on http2InterceptorsBasicTestsServerEP2 {

    resource function 'default .() returns string {
        return "Response from resource - test";
    }
}

@http:ServiceConfig{
    interceptors: [
        new LastResponseInterceptor(), new DefaultResponseErrorInterceptor(), new DefaultResponseInterceptor(), 
        new RequestInterceptorReturnsError(), new DefaultRequestInterceptor(), new LastRequestInterceptor()
    ]
}
service /responseErrorInterceptor2 on http2InterceptorsBasicTestsServerEP2 {

    resource function 'default .() returns string {
        return "Response from resource - test";
    }
}

@test:Config{}
function tesHttp2ResponseErrorInterceptor() returns error? {
    http:Response res = check http2InterceptorsBasicTestsClientEP2->get("/responseErrorInterceptor1");
    test:assertEquals(res.statusCode, 500);
    assertTextPayload(check res.getTextPayload(), "Response interceptor returns an error");
    assertHeaderValue(check res.getHeader("last-interceptor"), "default-response-error-interceptor");
    assertHeaderValue(check res.getHeader("last-response-interceptor"), "true");
    assertHeaderValue(check res.getHeader("default-response-error-interceptor"), "true");
    assertHeaderValue(check res.getHeader("error-type"), "NormalError");

    res = check http2InterceptorsBasicTestsClientEP2->get("/responseErrorInterceptor2");
    test:assertEquals(res.statusCode, 500);
    assertTextPayload(check res.getTextPayload(), "Request interceptor returns an error");
    assertHeaderValue(check res.getHeader("last-interceptor"), "default-response-error-interceptor");
    assertHeaderValue(check res.getHeader("last-response-interceptor"), "true");
    assertHeaderValue(check res.getHeader("default-response-error-interceptor"), "true");
    assertHeaderValue(check res.getHeader("error-type"), "NormalError");
}

@http:ServiceConfig {
    interceptors : [new DefaultRequestInterceptor(), new RequestInterceptorSetPayload(), new LastRequestInterceptor()]
}
service /requestInterceptorSetPayload on http2InterceptorsBasicTestsServerEP2 {

    resource function 'default .(http:Caller caller, http:Request req) returns error? {
        http:Response res = new();
        res.setTextPayload(check req.getTextPayload());
        res.setHeader("last-interceptor", check req.getHeader("last-interceptor"));
        res.setHeader("default-request-interceptor", check req.getHeader("default-request-interceptor"));
        res.setHeader("request-interceptor-setpayload", check req.getHeader("request-interceptor-setpayload"));
        res.setHeader("last-request-interceptor", check req.getHeader("last-request-interceptor"));
        check caller->respond(res);
    }
}

@test:Config{}
function tesHttp2RequestInterceptorSetPayload() returns error? {
    http:Request req = new();
    req.setHeader("interceptor", "databinding-interceptor");
    req.setTextPayload("Request from Client");
    http:Response res = check http2InterceptorsBasicTestsClientEP2->post("/requestInterceptorSetPayload", req);
    assertTextPayload(check res.getTextPayload(), "Text payload from request interceptor");
    assertHeaderValue(check res.getHeader("last-interceptor"), "request-interceptor-setpayload");
    assertHeaderValue(check res.getHeader("default-request-interceptor"), "true");
    assertHeaderValue(check res.getHeader("last-request-interceptor"), "true");
    assertHeaderValue(check res.getHeader("request-interceptor-setpayload"), "true");
}

int http2InterceptorBasicTestsPort3 = getHttp2Port(interceptorBasicTestsPort3);

final http:Client http2InterceptorsBasicTestsClientEP3 = check new("http://localhost:" + http2InterceptorBasicTestsPort3.toString(), 
    httpVersion = "2.0", http2Settings = { http2PriorKnowledge: true });

listener http:Listener http2InterceptorsBasicTestsServerEP3 = new(http2InterceptorBasicTestsPort3);

@http:ServiceConfig {
    interceptors : [new LastResponseInterceptor(), new ResponseInterceptorSetPayload(), new DefaultResponseInterceptor()]
}
service /responseInterceptorSetPayload on http2InterceptorsBasicTestsServerEP3 {

    resource function 'default .(http:Caller caller, http:Request req) returns error? {
        http:Response res = new();
        res.setTextPayload(check req.getTextPayload());
        check caller->respond(res);
    }
}

@test:Config{}
function tesHttp2ResponseInterceptorSetPayload() returns error? {
    http:Request req = new();
    req.setTextPayload("Request from Client");
    http:Response res = check http2InterceptorsBasicTestsClientEP3->post("/responseInterceptorSetPayload", req);
    assertTextPayload(check res.getTextPayload(), "Text payload from response interceptor");
    assertHeaderValue(check res.getHeader("last-interceptor"), "response-interceptor-setpayload");
    assertHeaderValue(check res.getHeader("default-response-interceptor"), "true");
    assertHeaderValue(check res.getHeader("last-response-interceptor"), "true");
    assertHeaderValue(check res.getHeader("response-interceptor-setpayload"), "true");
}

@http:ServiceConfig {
    interceptors : [new DefaultRequestInterceptor(), new RequestInterceptorReturnsResponse(), new LastRequestInterceptor()]
}
service /request on http2InterceptorsBasicTestsServerEP3 {

    resource function 'default .() returns string {
        return "Response from resource - test";
    }
}

@test:Config{}
function tesHttp2RequestInterceptorReturnsResponse() returns error? {
    http:Request req = new();
    req.setTextPayload("Request from Client");
    http:Response res = check http2InterceptorsBasicTestsClientEP3->post("/request", req);
    assertTextPayload(check res.getTextPayload(), "Response from Interceptor : Request from Client");
    assertHeaderValue(check res.getHeader("last-interceptor"), "default-request-interceptor");
    assertHeaderValue(check res.getHeader("request-interceptor-returns-response"), "true");
}

@http:ServiceConfig {
    interceptors : [new LastResponseInterceptor(), new ResponseInterceptorReturnsResponse(), new DefaultResponseInterceptor()]
}
service /response on http2InterceptorsBasicTestsServerEP3 {

    resource function 'default .() returns string {
        return "Response from resource - test";
    }
}

@test:Config{}
function tesHttp2ResponseInterceptorReturnsResponse() returns error? {
    http:Response res = check http2InterceptorsBasicTestsClientEP3->get("/response");
    assertTextPayload(check res.getTextPayload(), "Response from Interceptor : Response from resource - test");
    assertHeaderValue(check res.getHeader("last-interceptor"), "response-interceptor-returns-response");
    assertHeaderValue(check res.getHeader("default-response-interceptor"), "true");
    assertHeaderValue(check res.getHeader("last-response-interceptor"), "true");
    assertHeaderValue(check res.getHeader("response-interceptor-returns-response"), "true");
}

@http:ServiceConfig {
    interceptors : [
        new DefaultRequestInterceptor(), new GetRequestInterceptor(), new PostRequestInterceptor(), 
        new LastRequestInterceptor()
    ]
}
service /requestInterceptorHttpVerb on http2InterceptorsBasicTestsServerEP3 {

    resource function 'default .(http:Caller caller, http:Request req) returns error? {
        http:Response res = new();
        res.setHeader("last-interceptor", check req.getHeader("last-interceptor"));
        res.setHeader("default-request-interceptor", check req.getHeader("default-request-interceptor"));
        res.setHeader("last-request-interceptor", check req.getHeader("last-request-interceptor"));
        check caller->respond(res);
    }
}

@test:Config{}
function tesHttp2RequestInterceptorHttpVerb() returns error? {
    http:Response res = check http2InterceptorsBasicTestsClientEP3->get("/requestInterceptorHttpVerb");
    assertHeaderValue(check res.getHeader("last-interceptor"), "get-interceptor");
    assertHeaderValue(check res.getHeader("default-request-interceptor"), "true");
    assertHeaderValue(check res.getHeader("last-request-interceptor"), "true");

    res = check http2InterceptorsBasicTestsClientEP3->post("/requestInterceptorHttpVerb", "testMessage");
    assertHeaderValue(check res.getHeader("last-interceptor"), "post-interceptor");
    assertHeaderValue(check res.getHeader("default-request-interceptor"), "true");
    assertHeaderValue(check res.getHeader("last-request-interceptor"), "true");
}

int http2RequestInterceptorBasePathTestPort = getHttp2Port(requestInterceptorBasePathTestPort);

final http:Client http2RequestInterceptorBasePathClientEP = check new("http://localhost:" + http2RequestInterceptorBasePathTestPort.toString(), 
    httpVersion = "2.0", http2Settings = { http2PriorKnowledge: true });

listener http:Listener http2RequestInterceptorBasePathServerEP = new(http2RequestInterceptorBasePathTestPort);

@http:ServiceConfig {
    interceptors : [new DefaultRequestInterceptor(), new DefaultRequestInterceptorBasePath(), new LastRequestInterceptor()]
}
service / on http2RequestInterceptorBasePathServerEP {

    resource function 'default .(http:Caller caller, http:Request req) returns error? {
        http:Response res = new();
        res.setHeader("last-interceptor", check req.getHeader("last-interceptor"));
        res.setHeader("default-request-interceptor", check req.getHeader("default-request-interceptor"));
        res.setHeader("last-request-interceptor", check req.getHeader("last-request-interceptor"));
        check caller->respond(res);
    }

    resource function 'default foo(http:Caller caller, http:Request req) returns error? {
        http:Response res = new();
        res.setHeader("last-interceptor", check req.getHeader("last-interceptor"));
        res.setHeader("default-request-interceptor", check req.getHeader("default-request-interceptor"));
        res.setHeader("last-request-interceptor", check req.getHeader("last-request-interceptor"));
        check caller->respond(res);
    }
}

@test:Config{}
function tesHttp2RequestInterceptorBasePath() returns error? {
    http:Response res = check http2RequestInterceptorBasePathClientEP->get("/");
    assertHeaderValue(check res.getHeader("last-interceptor"), "default-request-interceptor");
    assertHeaderValue(check res.getHeader("default-request-interceptor"), "true");
    assertHeaderValue(check res.getHeader("last-request-interceptor"), "true");

    res = check http2RequestInterceptorBasePathClientEP->post("/foo", "testMessage");
    assertHeaderValue(check res.getHeader("last-interceptor"), "default-base-path-interceptor");
    assertHeaderValue(check res.getHeader("default-request-interceptor"), "true");
    assertHeaderValue(check res.getHeader("last-request-interceptor"), "true");
}

int http2GetRequestInterceptorBasePathTestPort = getHttp2Port(getRequestInterceptorBasePathTestPort);

final http:Client http2GetRequestInterceptorBasePathClientEP = check new("http://localhost:" + http2GetRequestInterceptorBasePathTestPort.toString(), 
    httpVersion = "2.0", http2Settings = { http2PriorKnowledge: true });

listener http:Listener http2GetRequestInterceptorBasePathServerEP = new(http2GetRequestInterceptorBasePathTestPort);

@http:ServiceConfig {
    interceptors : [new DefaultRequestInterceptor(), new GetRequestInterceptorBasePath(), new LastRequestInterceptor()]
}
service /foo on http2GetRequestInterceptorBasePathServerEP {

    resource function 'default .(http:Caller caller, http:Request req) returns error? {
        http:Response res = new();
        res.setHeader("last-interceptor", check req.getHeader("last-interceptor"));
        res.setHeader("default-request-interceptor", check req.getHeader("default-request-interceptor"));
        res.setHeader("last-request-interceptor", check req.getHeader("last-request-interceptor"));
        check caller->respond(res);
    }

    resource function 'default bar(http:Caller caller, http:Request req) returns error? {
        http:Response res = new();
        res.setHeader("last-interceptor", check req.getHeader("last-interceptor"));
        res.setHeader("default-request-interceptor", check req.getHeader("default-request-interceptor"));
        res.setHeader("last-request-interceptor", check req.getHeader("last-request-interceptor"));
        check caller->respond(res);
    }
}

@test:Config{}
function tesHttp2GetRequestInterceptorBasePath() returns error? {
    http:Response res = check http2GetRequestInterceptorBasePathClientEP->get("/foo");
    assertHeaderValue(check res.getHeader("last-interceptor"), "default-request-interceptor");
    assertHeaderValue(check res.getHeader("default-request-interceptor"), "true");
    assertHeaderValue(check res.getHeader("last-request-interceptor"), "true");

    res = check http2GetRequestInterceptorBasePathClientEP->get("/foo/bar");
    assertHeaderValue(check res.getHeader("last-interceptor"), "get-base-path-interceptor");
    assertHeaderValue(check res.getHeader("default-request-interceptor"), "true");
    assertHeaderValue(check res.getHeader("last-request-interceptor"), "true");

    res = check http2GetRequestInterceptorBasePathClientEP->post("/foo/bar", "testMessage");
    assertHeaderValue(check res.getHeader("last-interceptor"), "default-request-interceptor");
    assertHeaderValue(check res.getHeader("default-request-interceptor"), "true");
    assertHeaderValue(check res.getHeader("last-request-interceptor"), "true");
}