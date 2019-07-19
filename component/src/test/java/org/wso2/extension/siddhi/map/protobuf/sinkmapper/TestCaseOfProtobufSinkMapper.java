/*
 *  Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.wso2.extension.siddhi.map.protobuf.sinkmapper;


import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.transport.InMemoryBroker;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.map.protobuf.utils.service.SequenceCallRequest;

import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfProtobufSinkMapper {
    private static final Logger log = Logger.getLogger(TestCaseOfProtobufSinkMapper.class);
    private final int waitTime = 2000;
    private final int timeout = 30000;
    private AtomicInteger wso2Count = new AtomicInteger(0);
    private AtomicInteger ibmCount = new AtomicInteger(0);

    @BeforeMethod
    public void init() {
        wso2Count.set(0);
        ibmCount.set(0);
    }
    @Test
    public void jsonSinkMapperTestCase1() throws InterruptedException {
        log.info("JsonSinkMapperTestCase 1");
        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                String jsonString;
                switch (wso2Count.incrementAndGet()) {
                    case 1:
                        jsonString = "{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100}}";
                        AssertJUnit.assertEquals(jsonString , ((SequenceCallRequest) msg).getPayloadAsJSON());
                        break;
                    default:
                        AssertJUnit.fail();
                }
            }
            @Override
            public String getTopic() {
                return "WSO2";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='WSO2', @map(type='protobuf', mode='MIConnect')) " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Event wso2Event = new Event();
        Event ibmEvent = new Event();
        Object[] wso2Data = {"WSO2", 55.6f, 100L};
        Object[] ibmData = {"IBM", 32.6f, 160L};
        wso2Event.setData(wso2Data);
        ibmEvent.setData(ibmData);
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
    }
}
