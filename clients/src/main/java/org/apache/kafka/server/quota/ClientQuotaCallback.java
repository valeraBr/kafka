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
package org.apache.kafka.server.quota;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Map;

/**
 * Quota callback interface for brokers that enables customization of client quota computation.
 */
public interface ClientQuotaCallback extends Configurable {

    /**
     * Quota callback invoked to determine the quota limit to be applied for a request.
     *
     * @param principal The user principal of the connection for which quota is requested
     * @param clientId  The client id associated with the request
     * @param quotaType Type of quota requested
     * @return the quota including the limit and metric tags that indicate which other clients share this quota
     */
    ClientQuota quota(KafkaPrincipal principal, String clientId, ClientQuotaType quotaType);

    /**
     * Returns the quota limit associated with the provided metric tags. These tags were returned from
     * a previous call to {@link #quota(KafkaPrincipal, String, ClientQuotaType)}. This method is invoked
     * by quota managers to obtain the current quota limit applied to a metric after a quota update or
     * cluster metadata change. If the tags are no longer in use after the update, (e.g. this is a
     * {user, client-id} quota metric and the quota now in use is a {user} quota), null is returned.
     *
     * @param metricTags Metric tags for a quota metric of type `quotaType`
     * @param quotaType  Type of quota requested
     * @return the quota limit for the provided metric tags or null if the metric tags are no longer in use
     */
    Double quotaLimit(Map<String, String> metricTags, ClientQuotaType quotaType);

    /**
     * Metadata update callback that is invoked whenever UpdateMetadata request is received from
     * the controller. This is useful if quota computation takes partitions into account.
     * Topics that are being deleted will not be included in `cluster`.
     *
     * @param cluster Cluster metadata including partitions and their leaders if known
     * @return true if quotas have changed and metric configs may need to be updated
     */
    boolean updateClusterMetadata(Cluster cluster);

    /**
     * Quota configuration update callback that is invoked when quota configuration for an entity is
     * updated in ZooKeeper. This is useful to track configured quotas if built-in quota configuration
     * tools are used for quota management.
     *
     * @param quotaEntity The quota entity for which quota is being updated
     * @param quotaType   Type of quota being updated
     * @param newValue    The new quota value
     * @return true if quotas have changed and metric configs may need to be updated
     */
    boolean updateQuota(ClientQuotaEntity quotaEntity, ClientQuotaType quotaType, double newValue);

    /**
     * Quota configuration removal callback that is invoked when quota configuration for an entity is
     * removed in ZooKeeper. This is useful to track configured quotas if built-in quota configuration
     * tools are used for quota management.
     *
     * @param quotaEntity The quota entity for which quota is being updated.
     * @param quotaType   Type of quota being updated.
     * @return true if quotas have changed and metric configs may need to be updated
     */
    boolean removeQuota(ClientQuotaEntity quotaEntity, ClientQuotaType quotaType);

    /**
     * Closes this instance.
     */
    void close();
}

