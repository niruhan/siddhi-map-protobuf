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
package org.wso2.extension.siddhi.map.protobuf.utils;

import com.google.protobuf.DescriptorProtos;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Constants {
    public static final Map<DescriptorProtos.FieldDescriptorProto.Type, Integer> WIRE_TYPE_MAP;

    static {
        Map<DescriptorProtos.FieldDescriptorProto.Type, Integer> wireMap = new HashMap<>();
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE, 1);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, 5);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, 0);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64, 0);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32, 0);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64, 0);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32, 0);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64, 0);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32, 5);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED64, 1);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32, 5);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64, 1);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL, 0);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM, 0);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, 2);
        wireMap.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES, 2);
        WIRE_TYPE_MAP = Collections.unmodifiableMap(wireMap);
    }
}
