/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Provider;
import java.security.Security;
import java.util.Map;

public class SecurityUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(SecurityConfig.class);

    public static KafkaPrincipal parseKafkaPrincipal(String str) {
        if (str == null || str.isEmpty()) {
            throw new IllegalArgumentException("expected a string in format principalType:principalName but got " + str);
        }

        String[] split = str.split(":", 2);

        if (split.length != 2) {
            throw new IllegalArgumentException("expected a string in format principalType:principalName but got " + str);
        }

        return new KafkaPrincipal(split[0], split[1]);
    }

    public static void addConfiguredSecurityProviders(Map<String, ?> configs) {
        String securityProviderClassesStr = (String) configs.get(SecurityConfig.SECURITY_PROVIDER_CLASS_CONFIG);
        if (securityProviderClassesStr == null || securityProviderClassesStr.equals("")) {
            return;
        }
        try {
            String[] securityProviderClasses = securityProviderClassesStr.replaceAll("\\s+", "").split(",");
            for (int index = 0; index < securityProviderClasses.length; index++) {
                Security.insertProviderAt((Provider) Class.forName(securityProviderClasses[index]).newInstance(), index + 1);
            }
        } catch (ClassNotFoundException cnfe) {
            LOGGER.error("Unrecognized security provider class", cnfe);
        } catch (IllegalAccessException | InstantiationException e) {
            LOGGER.error("Unexpected implementation of security provider class", e);
        }
    }

}
