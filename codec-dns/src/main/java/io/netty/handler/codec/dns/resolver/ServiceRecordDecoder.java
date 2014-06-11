/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.dns.resolver;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.dns.DnsResource;
import io.netty.handler.codec.dns.DnsUtil;
import io.netty.util.internal.StringUtil;

/**
 * Decodes SRV (service) resource records.
 */
public final class ServiceRecordDecoder implements DnsResourceDecoder<ServiceRecord> {

    /**
     * Returns a decoded SRV (service) resource record, stored as an instance of
     * {@link ServiceRecord}.
     *
     * @param resource
     *            the resource record being decoded
     */
    @Override
    public ServiceRecord decode(DnsResource resource) {
        ByteBuf data = resource.content();
        int priority = data.readShort();
        int weight = data.readShort();
        int port = data.readUnsignedShort();
        String target = DnsUtil.readName(data);
        String[] parts = StringUtil.split(resource.name(), '.', 3);
        if (parts.length != 3) {
            throw new DecoderException("Invalid DnsResource name for a ServiceRecord: " + resource.name());
        }
        return new ServiceRecord(parts[0], parts[1], parts[2], priority, weight, port, target);
    }
}
