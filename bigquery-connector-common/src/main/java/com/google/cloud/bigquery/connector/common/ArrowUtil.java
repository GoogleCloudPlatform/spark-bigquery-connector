package com.google.cloud.bigquery.connector.common;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.NettyAllocationManager;

/** Common utility classes for Arrow. */
public class ArrowUtil {
  private ArrowUtil() {};

  /** Returns a new allocator limited to maxAllocation bytes */
  public static RootAllocator newRootAllocator(long maxAllocation) {
    // Arrow has classpath scanning logic based on hard-coding strings to
    // select the default allocationManagerFactory by selecting one
    // here it should be properly shaded with the rest of Arrow within
    // the connector.
    return new RootAllocator(
        RootAllocator.configBuilder()
            .allocationManagerFactory(NettyAllocationManager.FACTORY)
            .maxAllocation(maxAllocation)
            .build());
  }
}
