package io.rsocket.frame.ext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

public class CompositeMetadatasFlyweight {
  public static ByteBuf encode(ByteBufAllocator allocator, ByteBuf... metadata) {
    Objects.requireNonNull(allocator);
    Objects.requireNonNull(metadata);
    
    if (metadata.length == 1) {
      return metadata[0];
    } else {
      return allocator.compositeBuffer().addComponents(true, metadata);
    }
  }
  
  public static Iterable<ByteBuf> toIterable(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf);
    
    if (byteBuf.readableBytes() == 0) {
      return Collections.EMPTY_LIST;
    } else {
      return () ->
               new Iterator<ByteBuf>() {
                 ByteBuf slice = byteBuf.slice();
                 int index = slice.readerIndex();
                 int readableBytes = slice.readableBytes();
                 boolean hasNext = true;
          
                 @Override
                 public boolean hasNext() {
                   return hasNext;
                 }
          
                 @Override
                 public ByteBuf next() {
                   int length = CompositeMetadataFlyweight.length(slice);
                   index += length;
            
                   if (readableBytes == index) {
                     hasNext = false;
                     return slice.slice();
                   } else {
                     return slice.readSlice(length);
                   }
                 }
               };
    }
  }
  
}
