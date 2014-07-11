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
package io.netty.buffer;

import java.nio.ByteBuffer;

public interface ByteWriter {

    ByteWriter writeBoolean(boolean value);

    ByteWriter writeByte(int value);

    ByteWriter writeShort(int value);

    ByteWriter writeMedium(int value);

    ByteWriter writeInt(int value);

    ByteWriter writeLong(long value);

    ByteWriter writeChar(int value);

    ByteWriter writeFloat(float value);

    ByteWriter writeDouble(double value);

    ByteWriter writeBytes(ByteBuf src);

    ByteWriter writeBytes(ByteBuf src, int length);

    ByteWriter writeBytes(ByteBuf src, int srcIndex, int length);

    ByteWriter writeBytes(byte[] src);

    ByteWriter writeBytes(byte[] src, int srcIndex, int length);

    ByteWriter writeBytes(ByteBuffer src);
}
