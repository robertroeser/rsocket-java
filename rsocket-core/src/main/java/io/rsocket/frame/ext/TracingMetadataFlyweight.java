package io.rsocket.frame.ext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.Objects;
import java.util.UUID;

public class TracingMetadataFlyweight {
  private static final int HAS_128_BIT_TRACE_ID = 0b10000000;
  private static final int HAS_PARENT_ID = 0b01000000;
  private static final int HAS_SAMPLING_DECISION_SET = 0b00100000;
  private static final int SHOULD_SAMPLE_DECISION_REPORT = 0b00010000;
  private static final int HAS_DEBUG_SET = 0b00001000;
  private static final int SHOULD_REPORT_REPORT_TO_TRACING_SYSTEM = 0b00000100;

  private static ByteBuf encode(
      ByteBufAllocator allocator,
      boolean has128BitTraceId,
      boolean hasParentId,
      boolean hasSamplingDecisionSet,
      boolean shouldSamplingDecisionReport,
      boolean hasDebugSet,
      boolean shouldReportToTracingSystem,
      ByteBuf _128BitTraceId,
      long _64BitTraceId,
      long spanId,
      long parentId) {
    Objects.requireNonNull(allocator);

    if (spanId < 0) {
      throw new IllegalArgumentException("span id is negative");
    }

    int flags = 0;
    if (has128BitTraceId) {
      flags += HAS_128_BIT_TRACE_ID;
    }

    if (hasParentId) {
      flags += HAS_PARENT_ID;
    }

    if (hasSamplingDecisionSet) {
      flags += HAS_SAMPLING_DECISION_SET;
    }

    if (shouldSamplingDecisionReport) {
      flags += SHOULD_SAMPLE_DECISION_REPORT;
    }

    if (hasDebugSet) {
      flags += HAS_DEBUG_SET;
    }

    if (shouldReportToTracingSystem) {
      flags += SHOULD_REPORT_REPORT_TO_TRACING_SYSTEM;
    }

    ByteBuf byteBuf = allocator.buffer().writeByte(flags);

    if (has128BitTraceId) {
      _128BitTraceId.markReaderIndex();
      Objects.requireNonNull(_128BitTraceId);
      byteBuf.writeBytes(_128BitTraceId);
      _128BitTraceId.resetReaderIndex();
    } else {
      if (_64BitTraceId < 0) {
        throw new IllegalArgumentException("trace id is negative");
      }
      byteBuf.writeLong(_64BitTraceId);
    }

    byteBuf.writeLong(spanId);
    if (hasParentId) {
      if (parentId < 0) {
        throw new IllegalArgumentException("parent id is negative");
      }
      byteBuf.writeLong(parentId);
    }

    return byteBuf;
  }

  public static ByteBuf encode128BitWithParentId(
      ByteBufAllocator allocator,
      boolean hasSamplingDecisionSet,
      boolean shouldSamplingDecisionReport,
      boolean hasDebugSet,
      boolean shouldReportToTracingSystem,
      ByteBuf _128BitTraceId,
      long spanId,
      long parentId) {
    int readableBytes = _128BitTraceId.readableBytes();
    if (readableBytes != 16) {
      throw new IllegalArgumentException("128-bit Id must by 16 bytes, found: " + readableBytes);
    }

    return encode(
        allocator,
        true,
        true,
        hasSamplingDecisionSet,
        shouldSamplingDecisionReport,
        hasDebugSet,
        shouldReportToTracingSystem,
        _128BitTraceId,
        Long.MIN_VALUE,
        spanId,
        parentId);
  }

  public static ByteBuf encode128Bit(
      ByteBufAllocator allocator,
      boolean hasSamplingDecisionSet,
      boolean shouldSamplingDecisionReport,
      boolean hasDebugSet,
      boolean shouldReportToTracingSystem,
      UUID _128BitTraceId,
      long spanId) {
    ByteBuf byteBuf =
        allocator
            .buffer()
            .writeLong(_128BitTraceId.getMostSignificantBits())
            .writeLong(_128BitTraceId.getLeastSignificantBits());
    Objects.requireNonNull(_128BitTraceId, "128-bit trace id is null");
    return encode128Bit(
        allocator,
        hasSamplingDecisionSet,
        shouldSamplingDecisionReport,
        hasDebugSet,
        shouldReportToTracingSystem,
        byteBuf,
        spanId);
  }

  public static ByteBuf encode128Bit(
      ByteBufAllocator allocator,
      boolean hasSamplingDecisionSet,
      boolean shouldSamplingDecisionReport,
      boolean hasDebugSet,
      boolean shouldReportToTracingSystem,
      ByteBuf _128BitTraceId,
      long spanId) {
    int readableBytes = _128BitTraceId.readableBytes();
    if (readableBytes != 16) {
      throw new IllegalArgumentException("128-bit Id must by 16 bytes, found: " + readableBytes);
    }

    return encode(
        allocator,
        true,
        false,
        hasSamplingDecisionSet,
        shouldSamplingDecisionReport,
        hasDebugSet,
        shouldReportToTracingSystem,
        _128BitTraceId,
        Long.MIN_VALUE,
        spanId,
        Long.MIN_VALUE);
  }

  public static ByteBuf encode64BitWithParentId(
      ByteBufAllocator allocator,
      boolean hasSamplingDecisionSet,
      boolean shouldSamplingDecisionReport,
      boolean hasDebugSet,
      boolean shouldReportToTracingSystem,
      long _64BitTraceId,
      long spanId,
      long parentId) {
    return encode(
        allocator,
        false,
        true,
        hasSamplingDecisionSet,
        shouldSamplingDecisionReport,
        hasDebugSet,
        shouldReportToTracingSystem,
        null,
        _64BitTraceId,
        spanId,
        parentId);
  }

  public static ByteBuf encode64Bit(
      ByteBufAllocator allocator,
      boolean hasSamplingDecisionSet,
      boolean shouldSamplingDecisionReport,
      boolean hasDebugSet,
      boolean shouldReportToTracingSystem,
      long _64BitTraceId,
      long spanId) {
    return encode(
        allocator,
        false,
        false,
        hasSamplingDecisionSet,
        shouldSamplingDecisionReport,
        hasDebugSet,
        shouldReportToTracingSystem,
        null,
        _64BitTraceId,
        spanId,
        Long.MIN_VALUE);
  }

  public static int flags(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    int i = byteBuf.readUnsignedByte();
    byteBuf.resetReaderIndex();
    return i;
  }

  public static boolean has128BitTraceId(int flags) {
    return HAS_128_BIT_TRACE_ID == (flags & HAS_128_BIT_TRACE_ID);
  }

  public static boolean has128BitTraceId(ByteBuf byteBuf) {
    return has128BitTraceId(flags(byteBuf));
  }

  public static boolean hasParentId(int flags) {
    return HAS_PARENT_ID == (flags & HAS_PARENT_ID);
  }

  public static boolean hasParentId(ByteBuf byteBuf) {
    return hasParentId(flags(byteBuf));
  }

  public static boolean hasSamplingDecisionSet(int flags) {
    return HAS_SAMPLING_DECISION_SET == (flags & HAS_SAMPLING_DECISION_SET);
  }

  public static boolean hasSamplingDecisionSet(ByteBuf byteBuf) {
    return hasSamplingDecisionSet(flags(byteBuf));
  }

  public static boolean shouldSampleDecisionReport(int flags) {
    return SHOULD_SAMPLE_DECISION_REPORT == (flags & SHOULD_SAMPLE_DECISION_REPORT);
  }

  public static boolean shouldSampleDecisionReport(ByteBuf byteBuf) {
    return shouldSampleDecisionReport(flags(byteBuf));
  }

  public static boolean hasDebugSet(int flags) {
    return HAS_DEBUG_SET == (flags & HAS_DEBUG_SET);
  }

  public static boolean hasDebugSet(ByteBuf byteBuf) {
    return hasDebugSet(flags(byteBuf));
  }

  public static boolean shouldReportToTracingSystem(int flags) {
    return SHOULD_REPORT_REPORT_TO_TRACING_SYSTEM
        == (flags & SHOULD_REPORT_REPORT_TO_TRACING_SYSTEM);
  }

  public static boolean shouldReportToTracingSystem(ByteBuf byteBuf) {
    return shouldSampleDecisionReport(flags(byteBuf));
  }

  public static long _64BitId(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(1);
    long l = byteBuf.readLong();
    byteBuf.resetReaderIndex();
    return l;
  }

  public static ByteBuf _128BitId(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(1);
    ByteBuf id = byteBuf.readSlice(Long.BYTES * 2);
    byteBuf.resetReaderIndex();
    return id;
  }

  private static int traceIdLength(ByteBuf byteBuf) {
    return has128BitTraceId(byteBuf) ? Long.BYTES * 2 : Long.BYTES;
  }

  public static long spanId(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(1 + traceIdLength(byteBuf));
    long i = byteBuf.readLong();
    byteBuf.resetReaderIndex();
    return i;
  }

  public static long parentId(ByteBuf byteBuf) {
    int bytesToSkip = 1 + traceIdLength(byteBuf) + Long.BYTES;

    if ((bytesToSkip + Long.BYTES) > byteBuf.readableBytes()) {
      throw new IllegalArgumentException("byteBuf does not contain Parent Id");
    }

    byteBuf.markReaderIndex();
    byteBuf.skipBytes(bytesToSkip);
    long i = byteBuf.readLong();
    byteBuf.resetReaderIndex();
    return i;
  }
}
