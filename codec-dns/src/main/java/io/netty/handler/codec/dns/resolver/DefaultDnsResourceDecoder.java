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

import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.dns.DnsEntry;
import io.netty.handler.codec.dns.DnsResource;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles the decoding of resource records. Some default decoders are mapped to
 * their resource types in the map {@code decoders}.
 */
public class DefaultDnsResourceDecoder implements DnsResourceDecoder<Object> {
    private static final DnsResourceDecoder<?> TYPE_A_DECODER = new ARecordDecoder();
    private static final DnsResourceDecoder<?> TYPE_AAAA_DECODER = new AAAARecordDecoder();
    private static final DnsResourceDecoder<?> TYPE_MX_DECODER = new MailExchangerRecordDecoder();
    private static final DnsResourceDecoder<?> TYPE_TXT_DECODER = new TextDecoder();
    private static final DnsResourceDecoder<?> TYPE_SRV_DECODER = new ServiceRecordDecoder();
    private static final DnsResourceDecoder<?> TYPE_NS_DECODER = new DomainDecoder();
    private static final DnsResourceDecoder<?> TYPE_CNAME_DECODER = new DomainDecoder();
    private static final DnsResourceDecoder<?> TYPE_PTR_DECODER = new DomainDecoder();
    private static final DnsResourceDecoder<?> TYPE_SOA_DECODER = new StartOfAuthorityRecordDecoder();

    @Override
    public Object decode(DnsResource resource) {
        int type = resource.type();
        switch (type) {
            case DnsEntry.TYPE_A:
                return TYPE_A_DECODER.decode(resource);
            case DnsEntry.TYPE_AAAA:
                return TYPE_AAAA_DECODER.decode(resource);
            case DnsEntry.TYPE_MX:
                return TYPE_MX_DECODER.decode(resource);
            case DnsEntry.TYPE_TXT:
                return TYPE_TXT_DECODER.decode(resource);
            case DnsEntry.TYPE_SRV:
                return TYPE_SRV_DECODER.decode(resource);
            case DnsEntry.TYPE_NS:
                return TYPE_NS_DECODER.decode(resource);
            case DnsEntry.TYPE_CNAME:
                return TYPE_CNAME_DECODER.decode(resource);
            case DnsEntry.TYPE_PTR:
                return TYPE_PTR_DECODER.decode(resource);
            case DnsEntry.TYPE_SOA:
                return TYPE_SOA_DECODER.decode(resource);
            default:
                throw new DecoderException("Unsupported resource record type [id: " + type + "].");
        }
    }
}
