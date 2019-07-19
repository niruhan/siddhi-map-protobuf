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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.output.sink.SinkListener;
import io.siddhi.core.stream.output.sink.SinkMapper;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.core.util.transport.TemplateBuilder;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.map.protobuf.utils.service.SequenceCallRequest;

import java.util.Map;

/**
 * This is a sample class-level comment, explaining what the extension class does.
 */
@Extension(
        name = "protobuf",
        namespace = "sinkMapper",
        description = " ",
        parameters = {
                /*@Parameter(
                        name = " ",
                        description = " " ,
                        dynamic = false/true,
                        optional = true/false, defaultValue = " ",
                        type = {DataType.INT or DataType.BOOL or DataType.STRING or DataType.DOUBLE, }),*/
        },
        examples = {
                @Example(
                        syntax = " ",
                        description = " "
                )
        }
)

public class ProtobufSinkMapper extends SinkMapper {
    private static final Logger log = Logger.getLogger(ProtobufSinkMapper.class);
    private static final String EVENT_PARENT_TAG = "event";
    private static final String ENCLOSING_ELEMENT_IDENTIFIER = "enclosing.element";
    private static final String DEFAULT_ENCLOSING_ELEMENT = "$";
    private static final String JSON_VALIDATION_IDENTIFIER = "validate.json";
    private static final String JSON_EVENT_SEPERATOR = ",";
    private static final String JSON_KEYVALUE_SEPERATOR = ":";
    private static final String JSON_ARRAY_START_SYMBOL = "[";
    private static final String JSON_ARRAY_END_SYMBOL = "]";
    private static final String JSON_EVENT_START_SYMBOL = "{";
    private static final String JSON_EVENT_END_SYMBOL = "}";
    private static final String UNDEFINED = "undefined";

    private String[] attributeNameArray;
    private String enclosingElement = null;
    private boolean isJsonValidationEnabled = false;
    private boolean isMIConnect;
//    private JsonSinkMapper jsonSinkMapper

    /**
     * Returns a list of supported dynamic options (that means for each event value of the option can change) by
     * the transport
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }

    /**
     The initialization method for {@link SinkMapper}, which will be called before other methods and validate
     * the all configuration and getting the initial values.
     * @param streamDefinition       containing stream definition bind to the {@link SinkMapper}
     * @param optionHolder           Option holder containing static and dynamic configuration related
     *                               to the {@link SinkMapper}
     * @param map                    Unmapped payload for reference
     * @param configReader           to read the sink related system configuration.
     * @param siddhiAppContext       the context of the {@link io.siddhi.query.api.SiddhiApp} used to
     *                               get siddhi related utilty functions.
     */
    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, Map<String, TemplateBuilder> map,
                     ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.attributeNameArray = streamDefinition.getAttributeNameArray();
        isMIConnect = optionHolder.validateAndGetOption("mode").getValue().equalsIgnoreCase("MIConnect");
    }

    /**
     * Returns the list of classes which this sink can consume.
     * Based on the type of the sink, it may be limited to being able to publish specific type of classes.
     * For example, a {@link SinkMapper} of type event can convert to CSV file objects of type String or byte.
     * @return array of supported classes , if extension can support of any types of classes then return empty array .
     */
    @Override
    public Class[] getOutputEventClasses() {
        return new Class[0];
    }

    /**
     * Method to map the events and send them to {@link SinkListener} for publishing
     *
     * @param events                 {@link Event}s that need to be mapped
     * @param optionHolder           Option holder containing static and dynamic options related to the mapper
     * @param map                    To build the message payload based on the given template
     * @param sinkListener           {@link SinkListener} that will be called with the mapped events
     */
    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder, Map<String, TemplateBuilder> map,
                           SinkListener sinkListener) {

    }

    /**
     * Method to map the event and send it to {@link SinkListener} for publishing
     *
     * @param event                  {@link Event} that need to be mapped
     * @param optionHolder           Option holder containing static and dynamic options related to the mapper
     * @param map                    To build the message payload based on the given template
     * @param sinkListener           {@link SinkListener} that will be called with the mapped event
     */
    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder, Map<String, TemplateBuilder> map,
                           SinkListener sinkListener) {
        if (isMIConnect) {
            String payloadAsJSON = constructJsonForDefaultMapping(event);
            SequenceCallRequest.Builder requestBuilder = SequenceCallRequest.newBuilder();
            requestBuilder.setPayloadAsJSON(payloadAsJSON);
            requestBuilder.setSequenceName("mySeq");
            SequenceCallRequest sequenceCallRequest = requestBuilder.build();
            sinkListener.publish(sequenceCallRequest);
        }
    }

    private String constructJsonForDefaultMapping(Object eventObj) {
        StringBuilder sb = new StringBuilder();
        int numberOfOuterObjects;
        if (enclosingElement != null) {
            String[] nodeNames = enclosingElement.split("\\.");
            if (DEFAULT_ENCLOSING_ELEMENT.equals(nodeNames[0])) {
                numberOfOuterObjects = nodeNames.length - 1;
            } else {
                numberOfOuterObjects = nodeNames.length;
            }
            for (String nodeName : nodeNames) {
                if (!DEFAULT_ENCLOSING_ELEMENT.equals(nodeName)) {
                    sb.append(JSON_EVENT_START_SYMBOL).append("\"").append(nodeName).append("\"")
                            .append(JSON_KEYVALUE_SEPERATOR);
                }
            }
            if (eventObj instanceof Event) {
                Event event = (Event) eventObj;
                JsonObject jsonEvent = constructSingleEventForDefaultMapping(doPartialProcessing(event));
                sb.append(jsonEvent);
            } else if (eventObj instanceof Event[]) {
                JsonArray eventArray = new JsonArray();
                for (Event event : (Event[]) eventObj) {
                    eventArray.add(constructSingleEventForDefaultMapping(doPartialProcessing(event)));
                }
                sb.append(eventArray.toString());
            } else {
                log.error("Invalid object type. " + eventObj.toString() +
                        " cannot be converted to an event or event array. Hence dropping message.");
                return null;
            }
            for (int i = 0; i < numberOfOuterObjects; i++) {
                sb.append(JSON_EVENT_END_SYMBOL);
            }
            return sb.toString();
        } else {
            if (eventObj instanceof Event) {
                Event event = (Event) eventObj;
                JsonObject jsonEvent = constructSingleEventForDefaultMapping(doPartialProcessing(event));
                return jsonEvent.toString();
            } else if (eventObj instanceof Event[]) {
                JsonArray eventArray = new JsonArray();
                for (Event event : (Event[]) eventObj) {
                    eventArray.add(constructSingleEventForDefaultMapping(doPartialProcessing(event)));
                }
                return (eventArray.toString());
            } else {
                log.error("Invalid object type. " + eventObj.toString() +
                        " cannot be converted to an event or event array.");
                return null;
            }
        }
    }

    private String constructJsonForCustomMapping(Object eventObj, TemplateBuilder payloadTemplateBuilder) {
        StringBuilder sb = new StringBuilder();
        int numberOfOuterObjects = 0;
        if (enclosingElement != null) {
            String[] nodeNames = enclosingElement.split("\\.");
            if (DEFAULT_ENCLOSING_ELEMENT.equals(nodeNames[0])) {
                numberOfOuterObjects = nodeNames.length - 1;
            } else {
                numberOfOuterObjects = nodeNames.length;
            }
            for (String nodeName : nodeNames) {
                if (!DEFAULT_ENCLOSING_ELEMENT.equals(nodeName)) {
                    sb.append(JSON_EVENT_START_SYMBOL).append("\"").append(nodeName).append("\"")
                            .append(JSON_KEYVALUE_SEPERATOR);
                }
            }
            if (eventObj instanceof Event) {
                Event event = doPartialProcessing((Event) eventObj);
                sb.append(payloadTemplateBuilder.build(event));
            } else if (eventObj instanceof Event[]) {
                String jsonEvent;
                sb.append(JSON_ARRAY_START_SYMBOL);
                for (Event e : (Event[]) eventObj) {
                    jsonEvent = (String) payloadTemplateBuilder.build(doPartialProcessing(e));
                    if (jsonEvent != null) {
                        sb.append(jsonEvent).append(JSON_EVENT_SEPERATOR).append("\n");
                    }
                }
                sb.delete(sb.length() - 2, sb.length());
                sb.append(JSON_ARRAY_END_SYMBOL);
            } else {
                log.error("Invalid object type. " + eventObj.toString() +
                        " cannot be converted to an event or event array. Hence dropping message.");
                return null;
            }
            for (int i = 0; i < numberOfOuterObjects; i++) {
                sb.append(JSON_EVENT_END_SYMBOL);
            }
            return sb.toString();
        } else {
            if (eventObj.getClass() == Event.class) {
                return (String) payloadTemplateBuilder.build(doPartialProcessing((Event) eventObj));
            } else if (eventObj.getClass() == Event[].class) {
                String jsonEvent;
                sb.append(JSON_ARRAY_START_SYMBOL);
                for (Event event : (Event[]) eventObj) {
                    jsonEvent = (String) payloadTemplateBuilder.build(doPartialProcessing(event));
                    if (jsonEvent != null) {
                        sb.append(jsonEvent).append(JSON_EVENT_SEPERATOR).append("\n");
                    }
                }
                sb.delete(sb.length() - 2, sb.length());
                sb.append(JSON_ARRAY_END_SYMBOL);
                return sb.toString();
            } else {
                log.error("Invalid object type. " + eventObj.toString() +
                        " cannot be converted to an event or event array. Hence dropping message.");
                return null;
            }
        }
    }

    private JsonObject constructSingleEventForDefaultMapping(Event event) {
        Object[] data = event.getData();
        JsonObject jsonEventObject = new JsonObject();
        JsonObject innerParentObject = new JsonObject();
        String attributeName;
        Object attributeValue;
        Gson gson = new Gson();
        for (int i = 0; i < data.length; i++) {
            attributeName = attributeNameArray[i];
            attributeValue = data[i];
            if (attributeValue != null) {
                if (attributeValue.getClass() == String.class) {
                    innerParentObject.addProperty(attributeName, attributeValue.toString());
                } else if (attributeValue instanceof Number) {
                    innerParentObject.addProperty(attributeName, (Number) attributeValue);
                } else if (attributeValue instanceof Boolean) {
                    innerParentObject.addProperty(attributeName, (Boolean) attributeValue);
                } else if (attributeValue instanceof Map) {
                    if (!((Map) attributeValue).isEmpty()) {
                        innerParentObject.add(attributeName, gson.toJsonTree(attributeValue));
                    }
                }
            }
        }
        jsonEventObject.add(EVENT_PARENT_TAG, innerParentObject);
        return jsonEventObject;
    }

    private Event doPartialProcessing(Event event) {
        Object[] data = event.getData();
        for (int i = 0; i < data.length; i++) {
            if (data[i] == null) {
                data[i] = UNDEFINED;
            }
        }
        return event;
    }

    private static boolean isValidJson(String jsonInString) {
        try {
            new Gson().fromJson(jsonInString, Object.class);
            return true;
        } catch (com.google.gson.JsonSyntaxException ex) {
            return false;
        }
    }
}
