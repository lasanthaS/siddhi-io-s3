/*
 *  Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.siddhi.extension.io.s3.sink.internal;

import io.siddhi.extension.io.s3.sink.S3Sink;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;

/**
 * {@code RotationStrategy} provides an interface to be implemented by strategies used to publish events to S3 buckets.
 */
public interface RotationStrategy {

    void init(SinkConfig config, ServiceClient client, S3Sink.SinkState taskQueue);

    void queueEvent(String objectPath, Object event);

    String getName();
}
