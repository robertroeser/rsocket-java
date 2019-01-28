package io.rsocket.frame.ext;

import org.junit.Assert;
import org.junit.Test;

public class WellKnownMimeTypeTest {
  @Test
  public void testFromMimeTypes() {
    WellKnownMimeType wellKnownMimeType = WellKnownMimeType
                                              .fromMimeType("application/avro");

    Assert.assertTrue(wellKnownMimeType.equals(WellKnownMimeType.application_avro));
  }
  
  @Test
  public void testFromCode() {
    WellKnownMimeType wellKnownMimeType = WellKnownMimeType.fromCode(1);
    Assert.assertTrue(wellKnownMimeType.equals(WellKnownMimeType.application_cbor));
  }
}