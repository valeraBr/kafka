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
package org.apache.kafka.common.network;

import org.apache.kafka.common.utils.Utils;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import java.nio.channels.spi.SelectorProvider;
import java.util.ConcurrentModificationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class NetworkContext {

    private static final SelectorProvider SELECTOR_PROVIDER_DEFAULT = SelectorProvider.provider();
    private static final SocketFactory SOCKET_FACTORY_DEFAULT = SocketFactory.getDefault();
    private static final ServerSocketFactory SERVER_SOCKET_FACTORY_DEFAULT = ServerSocketFactory.getDefault();
    private static final ReentrantLock LOCK = new ReentrantLock();
    private static volatile SelectorProvider selectorProvider = SELECTOR_PROVIDER_DEFAULT;
    private static volatile SocketFactory socketFactory = SOCKET_FACTORY_DEFAULT;
    private static volatile ServerSocketFactory serverSocketFactory = SERVER_SOCKET_FACTORY_DEFAULT;

    public static SelectorProvider provider() {
        return selectorProvider;
    }

    public static SocketFactory factory() {
        return socketFactory;
    }

    public static ServerSocketFactory serverFactory() {
        return serverSocketFactory;
    }

    /**
     * Temporarily install factories for network resources. When finished, close the returned resource to restore the
     * default factories. While the returned resource is open, the caller will have exclusive ownership over the
     * factories, and subsequent calls will fail with {@link ConcurrentModificationException}
     * <p>This is meant for use only in tests. Installation is non-atomic, so network resources may be created with the
     * default factories after installation, or the specified factories after uninstallation.
     * @param provider A provider for NIO selectors and sockets, maybe null
     * @param factory A provider for client-side TCP sockets
     * @param serverFactory A provider for server-side TCP sockets
     * @return An AutoClosable that when closed, restores the default factories. Closing is idempotent.
     */
    public static Utils.UncheckedCloseable install(SelectorProvider provider, SocketFactory factory, ServerSocketFactory serverFactory) {
        boolean active = LOCK.tryLock();
        if (active) {
            if (provider != null) {
                selectorProvider = provider;
            }
            if (factory != null) {
                socketFactory = factory;
            }
            if (serverFactory != null) {
                serverSocketFactory = serverFactory;
            }
        } else {
            throw new ConcurrentModificationException("The network context is already in-use");
        }
        AtomicBoolean canUninstall = new AtomicBoolean(true);
        return () -> {
            if (canUninstall.getAndSet(false)) {
                selectorProvider = SELECTOR_PROVIDER_DEFAULT;
                socketFactory = SOCKET_FACTORY_DEFAULT;
                serverSocketFactory = SERVER_SOCKET_FACTORY_DEFAULT;
                LOCK.unlock();
            }
        };
    }
}
