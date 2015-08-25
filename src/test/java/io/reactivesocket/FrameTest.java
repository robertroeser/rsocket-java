/**
 * Copyright 2015 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import io.reactivesocket.internal.SetupFrameFlyweight;
import org.junit.Test;

public class FrameTest
{
    @Test
    public void testWriteThenRead() {
    	final ByteBuffer helloBuffer = TestUtil.byteBufferFromUtf8String("hello");
        
        Frame f = Frame.from(1, FrameType.REQUEST_RESPONSE, helloBuffer);
        assertEquals("hello", TestUtil.byteToString(f.getData()));
        assertEquals(FrameType.REQUEST_RESPONSE, f.getType());
        assertEquals(1, f.getStreamId());

        ByteBuffer b = f.getByteBuffer();

        Frame f2 = Frame.from(b);
        assertEquals("hello", TestUtil.byteToString(f2.getData()));
        assertEquals(FrameType.REQUEST_RESPONSE, f2.getType());
        assertEquals(1, f2.getStreamId());
    }

    @Test
    public void testWrapMessage() {
    	final ByteBuffer helloBuffer = TestUtil.byteBufferFromUtf8String("hello");
        final ByteBuffer doneBuffer = TestUtil.byteBufferFromUtf8String("done");

        Frame f = Frame.from(1, FrameType.REQUEST_RESPONSE, helloBuffer);

        f.wrap(2, FrameType.COMPLETE, doneBuffer);
        assertEquals("done", TestUtil.byteToString(f.getData()));
        assertEquals(FrameType.NEXT_COMPLETE, f.getType());
        assertEquals(2, f.getStreamId());
    }

    @Test
    public void testWrapBytes() {
    	final ByteBuffer helloBuffer = TestUtil.byteBufferFromUtf8String("hello");
        final ByteBuffer anotherBuffer = TestUtil.byteBufferFromUtf8String("another");
        
        Frame f = Frame.from(1, FrameType.REQUEST_RESPONSE, helloBuffer);
        Frame f2 = Frame.from(20, FrameType.COMPLETE, anotherBuffer);

        ByteBuffer b = f2.getByteBuffer();
        f.wrap(b);

        assertEquals("another", TestUtil.byteToString(f.getData()));
        assertEquals(FrameType.NEXT_COMPLETE, f.getType());
        assertEquals(20, f.getStreamId());
    }

    @Test
    public void shouldReturnCorrectDataPlusMetadataForRequestResponse()
    {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");
        final ByteBuffer requestMetadata = TestUtil.byteBufferFromUtf8String("request metadata");

        Frame f = Frame.from(1, FrameType.REQUEST_RESPONSE, requestData, requestMetadata);
        assertEquals("request data", TestUtil.byteToString(f.getData()));
        assertEquals("request metadata", TestUtil.byteToString(f.getMetadata()));
        assertEquals(FrameType.REQUEST_RESPONSE, f.getType());
        assertEquals(1, f.getStreamId());
    }

    @Test
    public void shouldReturnCorrectDataPlusMetadataForFireAndForget()
    {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");
        final ByteBuffer requestMetadata = TestUtil.byteBufferFromUtf8String("request metadata");

        Frame f = Frame.from(1, FrameType.FIRE_AND_FORGET, requestData, requestMetadata);
        assertEquals("request data", TestUtil.byteToString(f.getData()));
        assertEquals("request metadata", TestUtil.byteToString(f.getMetadata()));
        assertEquals(FrameType.FIRE_AND_FORGET, f.getType());
        assertEquals(1, f.getStreamId());
    }

    @Test
    public void shouldReturnCorrectDataPlusMetadataForRequestStream()
    {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");
        final ByteBuffer requestMetadata = TestUtil.byteBufferFromUtf8String("request metadata");

        Frame f = Frame.from(1, FrameType.REQUEST_STREAM, requestData, requestMetadata);
        assertEquals("request data", TestUtil.byteToString(f.getData()));
        assertEquals("request metadata", TestUtil.byteToString(f.getMetadata()));
        assertEquals(FrameType.REQUEST_STREAM, f.getType());
        assertEquals(1, f.getStreamId());
    }

    @Test
    public void shouldReturnCorrectDataPlusMetadataForRequestSubscription()
    {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");
        final ByteBuffer requestMetadata = TestUtil.byteBufferFromUtf8String("request metadata");

        Frame f = Frame.from(1, FrameType.REQUEST_SUBSCRIPTION, requestData, requestMetadata);
        assertEquals("request data", TestUtil.byteToString(f.getData()));
        assertEquals("request metadata", TestUtil.byteToString(f.getMetadata()));
        assertEquals(FrameType.REQUEST_SUBSCRIPTION, f.getType());
        assertEquals(1, f.getStreamId());
    }

    @Test
    public void shouldReturnCorrectDataPlusMetadataForResponse()
    {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("response data");
        final ByteBuffer requestMetadata = TestUtil.byteBufferFromUtf8String("response metadata");

        Frame f = Frame.from(1, FrameType.RESPONSE, requestData, requestMetadata);
        assertEquals("response data", TestUtil.byteToString(f.getData()));
        assertEquals("response metadata", TestUtil.byteToString(f.getMetadata()));
        assertEquals(FrameType.NEXT, f.getType());
        assertEquals(1, f.getStreamId());
    }

    @Test
    public void shouldReturnCorrectDataWithoutMetadataForRequestResponse()
    {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");

        Frame f = Frame.from(1, FrameType.REQUEST_RESPONSE, requestData);
        assertEquals("request data", TestUtil.byteToString(f.getData()));

        final ByteBuffer metadataBuffer = f.getMetadata();
        assertEquals(0, metadataBuffer.capacity());
        assertEquals(FrameType.REQUEST_RESPONSE, f.getType());
        assertEquals(1, f.getStreamId());
    }

    @Test
    public void shouldReturnCorrectDataWithoutMetadataForFireAndForget()
    {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");

        Frame f = Frame.from(1, FrameType.FIRE_AND_FORGET, requestData);
        assertEquals("request data", TestUtil.byteToString(f.getData()));

        final ByteBuffer metadataBuffer = f.getMetadata();
        assertEquals(0, metadataBuffer.capacity());
        assertEquals(FrameType.FIRE_AND_FORGET, f.getType());
        assertEquals(1, f.getStreamId());
    }

    @Test
    public void shouldReturnCorrectDataWithoutMetadataForRequestStream()
    {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");

        Frame f = Frame.from(1, FrameType.REQUEST_STREAM, requestData);
        assertEquals("request data", TestUtil.byteToString(f.getData()));

        final ByteBuffer metadataBuffer = f.getMetadata();
        assertEquals(0, metadataBuffer.capacity());
        assertEquals(FrameType.REQUEST_STREAM, f.getType());
        assertEquals(1, f.getStreamId());
    }

    @Test
    public void shouldReturnCorrectDataWithoutMetadataForRequestSubscription()
    {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");

        Frame f = Frame.from(1, FrameType.REQUEST_SUBSCRIPTION, requestData);
        assertEquals("request data", TestUtil.byteToString(f.getData()));

        final ByteBuffer metadataBuffer = f.getMetadata();
        assertEquals(0, metadataBuffer.capacity());
        assertEquals(FrameType.REQUEST_SUBSCRIPTION, f.getType());
        assertEquals(1, f.getStreamId());
    }

    @Test
    public void shouldReturnCorrectDataWithoutMetadataForResponse()
    {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("response data");

        Frame f = Frame.from(1, FrameType.RESPONSE, requestData);
        assertEquals("response data", TestUtil.byteToString(f.getData()));

        final ByteBuffer metadataBuffer = f.getMetadata();
        assertEquals(0, metadataBuffer.capacity());
        assertEquals(FrameType.NEXT, f.getType());
        assertEquals(1, f.getStreamId());
    }

    @Test
    public void shouldReturnCorrectDataPlusMetadataForSetup()
    {
        final int flags = SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE | SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION;
        final int keepaliveInterval = 1001;
        final int maxLifetime = keepaliveInterval * 5;
        final String metadataMimeType = "application/json";
        final String dataMimeType = "application/cbor";
        final ByteBuffer setupData = TestUtil.byteBufferFromUtf8String("setup data");
        final ByteBuffer setupMetadata = TestUtil.byteBufferFromUtf8String("setup metadata");

        Frame f = Frame.fromSetup(flags, keepaliveInterval, maxLifetime, metadataMimeType, dataMimeType, new Payload()
        {
            public ByteBuffer getData()
            {
                return setupData;
            }

            public ByteBuffer getMetadata()
            {
                return setupMetadata;
            }
        });

        assertEquals(FrameType.SETUP, f.getType());
        assertEquals(flags, Frame.Setup.getFlags(f));
        assertEquals(keepaliveInterval, Frame.Setup.keepaliveInterval(f));
        assertEquals(maxLifetime, Frame.Setup.maxLifetime(f));
        assertEquals(metadataMimeType, Frame.Setup.metadataMimeType(f));
        assertEquals(dataMimeType, Frame.Setup.dataMimeType(f));
        assertEquals("setup data", TestUtil.byteToString(f.getData()));
        assertEquals("setup metadata", TestUtil.byteToString(f.getMetadata()));
    }

    @Test
    public void shouldReturnCorrectDataWithoutMetadataForSetup()
    {
        final int flags = SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE | SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION;
        final int keepaliveInterval = 1001;
        final int maxLifetime = keepaliveInterval * 5;
        final String metadataMimeType = "application/json";
        final String dataMimeType = "application/cbor";
        final ByteBuffer setupData = TestUtil.byteBufferFromUtf8String("setup data");

        Frame f = Frame.fromSetup(flags, keepaliveInterval, maxLifetime, metadataMimeType, dataMimeType, new Payload()
        {
            public ByteBuffer getData()
            {
                return setupData;
            }

            public ByteBuffer getMetadata()
            {
                return Frame.NULL_BYTEBUFFER;
            }
        });

        assertEquals(FrameType.SETUP, f.getType());
        assertEquals(flags, Frame.Setup.getFlags(f));
        assertEquals(keepaliveInterval, Frame.Setup.keepaliveInterval(f));
        assertEquals(maxLifetime, Frame.Setup.maxLifetime(f));
        assertEquals(metadataMimeType, Frame.Setup.metadataMimeType(f));
        assertEquals(dataMimeType, Frame.Setup.dataMimeType(f));
        assertEquals("setup data", TestUtil.byteToString(f.getData()));
        assertEquals(Frame.NULL_BYTEBUFFER, f.getMetadata());
    }

    @Test
    public void shouldFormCorrectlyWithoutDataNorMetadataForSetup()
    {
        final int flags = SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE | SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION;
        final int keepaliveInterval = 1001;
        final int maxLifetime = keepaliveInterval * 5;
        final String metadataMimeType = "application/json";
        final String dataMimeType = "application/cbor";

        Frame f = Frame.fromSetup(flags, keepaliveInterval, maxLifetime, metadataMimeType, dataMimeType, new Payload()
        {
            public ByteBuffer getData()
            {
                return Frame.NULL_BYTEBUFFER;
            }

            public ByteBuffer getMetadata()
            {
                return Frame.NULL_BYTEBUFFER;
            }
        });

        assertEquals(FrameType.SETUP, f.getType());
        assertEquals(flags, Frame.Setup.getFlags(f));
        assertEquals(keepaliveInterval, Frame.Setup.keepaliveInterval(f));
        assertEquals(maxLifetime, Frame.Setup.maxLifetime(f));
        assertEquals(metadataMimeType, Frame.Setup.metadataMimeType(f));
        assertEquals(dataMimeType, Frame.Setup.dataMimeType(f));
        assertEquals(Frame.NULL_BYTEBUFFER, f.getData());
        assertEquals(Frame.NULL_BYTEBUFFER, f.getMetadata());
    }

    @Test
    public void shouldReturnCorrectDataPlusMetadataForSetupError()
    {
        final int errorCode = 42;
        final String metadata = "error metadata";
        final String data = "error data";

        Frame f = Frame.fromSetupError(errorCode, metadata, data);

        assertEquals(FrameType.SETUP_ERROR, f.getType());
        assertEquals(errorCode, Frame.SetupError.errorCode(f));
        assertEquals(data, TestUtil.byteToString(f.getData()));
        assertEquals(metadata, TestUtil.byteToString(f.getMetadata()));
    }

    @Test
    public void shouldReturnCorrectDataWithoutMetadataForSetupError()
    {
        final int errorCode = 42;
        final String metadata = "";
        final String data = "error data";

        Frame f = Frame.fromSetupError(errorCode, metadata, data);

        assertEquals(FrameType.SETUP_ERROR, f.getType());
        assertEquals(errorCode, Frame.SetupError.errorCode(f));
        assertEquals(data, TestUtil.byteToString(f.getData()));
        assertEquals(Frame.NULL_BYTEBUFFER, f.getMetadata());
    }

    @Test
    public void shouldFormCorrectlyWithoutDataNorMetadataForSetupError()
    {
        final int errorCode = 42;
        final String metadata = "";
        final String data = "";

        Frame f = Frame.fromSetupError(errorCode, metadata, data);

        assertEquals(FrameType.SETUP_ERROR, f.getType());
        assertEquals(errorCode, Frame.SetupError.errorCode(f));
        assertEquals(Frame.NULL_BYTEBUFFER, f.getData());
        assertEquals(Frame.NULL_BYTEBUFFER, f.getMetadata());
    }

}