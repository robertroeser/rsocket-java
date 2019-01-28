package io.rsocket.frame.ext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class TracingMetadataFlyweightTest {
  static ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

  @Test
  public void testIntegerFlags() {
    int flags = 0xFC;

    Assert.assertTrue(TracingMetadataFlyweight.has128BitTraceId(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasParentId(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasSamplingDecisionSet(flags));
    Assert.assertTrue(TracingMetadataFlyweight.shouldReportToTracingSystem(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasDebugSet(flags));
    Assert.assertTrue(TracingMetadataFlyweight.shouldSampleDecisionReport(flags));
  }

  @Test
  public void testBinaryFlags() {
    ByteBuf flags = allocator.buffer().writeByte(0xFC);

    Assert.assertTrue(TracingMetadataFlyweight.has128BitTraceId(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasParentId(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasSamplingDecisionSet(flags));
    Assert.assertTrue(TracingMetadataFlyweight.shouldReportToTracingSystem(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasDebugSet(flags));
    Assert.assertTrue(TracingMetadataFlyweight.shouldSampleDecisionReport(flags));
  }

  @Test
  public void testEncodeAndDecode128BitWithParentId() {
    long upper = Math.abs(ThreadLocalRandom.current().nextLong());
    long lower = Math.abs(ThreadLocalRandom.current().nextLong());
    long spanId = Math.abs(ThreadLocalRandom.current().nextLong());
    long parentId = Math.abs(ThreadLocalRandom.current().nextLong());

    ByteBuf _128BitId = allocator.buffer().writeLong(upper).writeLong(lower);

    ByteBuf byteBuf =
        TracingMetadataFlyweight.encode128BitWithParentId(
            allocator, true, false, true, false, _128BitId, spanId, parentId);

    int flags = TracingMetadataFlyweight.flags(byteBuf);

    Assert.assertTrue(TracingMetadataFlyweight.hasParentId(flags));
    Assert.assertTrue(TracingMetadataFlyweight.has128BitTraceId(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasSamplingDecisionSet(flags));
    Assert.assertFalse(TracingMetadataFlyweight.shouldSampleDecisionReport(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasDebugSet(flags));
    Assert.assertFalse(TracingMetadataFlyweight.shouldReportToTracingSystem(flags));

    ByteBuf id = TracingMetadataFlyweight._128BitId(byteBuf);

    Assert.assertEquals(ByteBufUtil.hexDump(_128BitId), ByteBufUtil.hexDump(id));
    Assert.assertEquals(spanId, TracingMetadataFlyweight.spanId(byteBuf));
    Assert.assertEquals(parentId, TracingMetadataFlyweight.parentId(byteBuf));
  }

  @Test
  public void testEncodeAndDecode128BitNoParentId() {
    long upper = Math.abs(ThreadLocalRandom.current().nextLong());
    long lower = Math.abs(ThreadLocalRandom.current().nextLong());
    long spanId = Math.abs(ThreadLocalRandom.current().nextLong());

    ByteBuf _128BitId = allocator.buffer().writeLong(upper).writeLong(lower);

    ByteBuf byteBuf =
        TracingMetadataFlyweight.encode128Bit(
            allocator, true, false, true, false, _128BitId, spanId);

    int flags = TracingMetadataFlyweight.flags(byteBuf);

    Assert.assertFalse(TracingMetadataFlyweight.hasParentId(flags));
    Assert.assertTrue(TracingMetadataFlyweight.has128BitTraceId(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasSamplingDecisionSet(flags));
    Assert.assertFalse(TracingMetadataFlyweight.shouldSampleDecisionReport(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasDebugSet(flags));
    Assert.assertFalse(TracingMetadataFlyweight.shouldReportToTracingSystem(flags));

    ByteBuf id = TracingMetadataFlyweight._128BitId(byteBuf);

    Assert.assertEquals(ByteBufUtil.hexDump(_128BitId), ByteBufUtil.hexDump(id));
    Assert.assertEquals(spanId, TracingMetadataFlyweight.spanId(byteBuf));
  }

  @Test
  public void testEncodeAndDecodeWithUUID() {
    long spanId = Math.abs(ThreadLocalRandom.current().nextLong());

    UUID _128BitId = UUID.randomUUID();

    ByteBuf byteBuf =
        TracingMetadataFlyweight.encode128Bit(
            allocator, true, false, true, false, _128BitId, spanId);

    int flags = TracingMetadataFlyweight.flags(byteBuf);

    Assert.assertFalse(TracingMetadataFlyweight.hasParentId(flags));
    Assert.assertTrue(TracingMetadataFlyweight.has128BitTraceId(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasSamplingDecisionSet(flags));
    Assert.assertFalse(TracingMetadataFlyweight.shouldSampleDecisionReport(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasDebugSet(flags));
    Assert.assertFalse(TracingMetadataFlyweight.shouldReportToTracingSystem(flags));

    ByteBuf id = TracingMetadataFlyweight._128BitId(byteBuf);

    UUID uuid = new UUID(id.readLong(), id.readLong());
    
    Assert.assertEquals(_128BitId, uuid);
    Assert.assertEquals(spanId, TracingMetadataFlyweight.spanId(byteBuf));
  }

  @Test
  public void testEncodeAndDecode64BitWithParentId() {
    long _64BitId = Math.abs(ThreadLocalRandom.current().nextLong());
    long spanId = Math.abs(ThreadLocalRandom.current().nextLong());
    long parentId = Math.abs(ThreadLocalRandom.current().nextLong());

    ByteBuf byteBuf =
        TracingMetadataFlyweight.encode64BitWithParentId(
            allocator, true, false, true, false, _64BitId, spanId, parentId);

    int flags = TracingMetadataFlyweight.flags(byteBuf);

    Assert.assertTrue(TracingMetadataFlyweight.hasParentId(flags));
    Assert.assertFalse(TracingMetadataFlyweight.has128BitTraceId(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasSamplingDecisionSet(flags));
    Assert.assertFalse(TracingMetadataFlyweight.shouldSampleDecisionReport(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasDebugSet(flags));
    Assert.assertFalse(TracingMetadataFlyweight.shouldReportToTracingSystem(flags));

    long id = TracingMetadataFlyweight._64BitId(byteBuf);

    Assert.assertEquals(_64BitId, id);
    Assert.assertEquals(spanId, TracingMetadataFlyweight.spanId(byteBuf));
    Assert.assertEquals(parentId, TracingMetadataFlyweight.parentId(byteBuf));
  }

  @Test
  public void testEncodeAndDecode64Bit() {
    long _64BitId = Math.abs(ThreadLocalRandom.current().nextLong());
    long spanId = Math.abs(ThreadLocalRandom.current().nextLong());

    ByteBuf byteBuf =
        TracingMetadataFlyweight.encode64Bit(allocator, true, false, true, false, _64BitId, spanId);

    int flags = TracingMetadataFlyweight.flags(byteBuf);

    Assert.assertFalse(TracingMetadataFlyweight.hasParentId(flags));
    Assert.assertFalse(TracingMetadataFlyweight.has128BitTraceId(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasSamplingDecisionSet(flags));
    Assert.assertFalse(TracingMetadataFlyweight.shouldSampleDecisionReport(flags));
    Assert.assertTrue(TracingMetadataFlyweight.hasDebugSet(flags));
    Assert.assertFalse(TracingMetadataFlyweight.shouldReportToTracingSystem(flags));

    long id = TracingMetadataFlyweight._64BitId(byteBuf);

    Assert.assertEquals(_64BitId, id);
    Assert.assertEquals(spanId, TracingMetadataFlyweight.spanId(byteBuf));
  }
}
