/*
* Copyright 2014 The Netty Project
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
package io.netty.handler.codec;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.VoidChannelPromise;
import io.netty.util.Recycler;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

final class CodecList implements List<Object> {

    private final Node head = new Node();
    private Node tail = head;

    private int size;

    private static final Recycler<CodecList> RECYCLER = new Recycler<CodecList>() {
        @Override
        protected CodecList newObject(Handle<CodecList> handle) {
            return new CodecList(handle);
        }
    };

    /**
     * Create a new empty {@link CodecList} instance
     */
    public static CodecList newInstance() {
        return RECYCLER.get();
    }

    private final Recycler.Handle<CodecList> handle;

    private CodecList(Recycler.Handle<CodecList> handle) {
        this.handle = handle;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return head.entry == null;
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) {
            return false;
        }
        Node node = head;
        while (node != null && node.entry != null) {
            if (node.entry.equals(o)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<Object> iterator() {
        return new Iterator<Object>() {
            private Node node = head;
            @Override
            public boolean hasNext() {
                return node.entry != null;
            }

            @Override
            public Object next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Object entry = node.entry;
                node = node.next;
                return entry;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(Object element) {
        if (element == null) {
            throw new NullPointerException("element");
        }
        Node node = tail;
        while (node.entry != null) {
            if (node.next == null) {
                node.next = new Node();
            }
            node = node.next;
        }
        node.entry = element;
        tail = node;
        size++;
        return true;
    }

    @Override
    public boolean remove(Object o) {
        if (o == null) {
            return false;
        }
        Node prev = null;
        Node node = head;
        while (node != null && node.entry != null) {
            if (node.entry.equals(o)) {
                if (prev == null) {
                    node.entry = null;
                } else {
                    prev.next = node.next;
                    if (prev.next == null) {
                        tail = prev;
                    }
                }
                size--;
                return true;
            }
            node = node.next;
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object obj: c) {
            if (!contains(obj)) {
                return false;
            }
        }
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends Object> c) {
        for (Object obj: c) {
            if (!add(obj)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(int index, Collection<? extends Object> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        Node node = head;
        while (node != null && node.entry != null) {
            node.entry = null;
            node = node.next;
        }
        tail = head;
        size = 0;
    }

    @Override
    public Object get(int index) {
        int i = 0;
        Node node = head;
        while (node != null && node.entry != null) {
            if (i == index) {
                return node.entry;
            }
            node = node.next;
            i++;
        }
        throw new IndexOutOfBoundsException();
    }

    @Override
    public Object set(int index, Object element) {
        int i = 0;
        Node node = head;
        while (node != null && node.entry != null) {
            if (i == index) {
                Object entry =  node.entry;
                node.entry = element;
                return entry;
            }
            node = node.next;
            i++;
        }
        throw new IndexOutOfBoundsException();
    }

    @Override
    public void add(int index, Object element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<Object> listIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<Object> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Object> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    void fireChannelRead(ChannelHandlerContext ctx) {
        try {
            Node node = head;
            while (node != null && node.entry != null) {
                ctx.fireChannelRead(node.entry);
                node.entry = null;
                node = node.next;
            }
        } finally {
            recycle();
        }
    }

    void write(ChannelHandlerContext ctx, ChannelPromise promise) {
        boolean isVoidPromise = promise instanceof VoidChannelPromise;
        try {
            Node node = head;
            while (node.entry != null) {
                Object entry = node.entry;
                node.entry = null;
                node = node.next;
                if (node == null || node.entry == null) {
                    ctx.write(entry, promise);
                    break;
                } else {
                    if (isVoidPromise) {
                        ctx.write(entry, ctx.voidPromise());
                    } else {
                        ctx.write(entry);
                    }
                }
            }
        } finally {
            recycle();
        }
    }

    void recycle() {
        clear();
        RECYCLER.recycle(this, handle);
    }

    private static final class Node {
        Object entry;
        Node next;
    }
}
