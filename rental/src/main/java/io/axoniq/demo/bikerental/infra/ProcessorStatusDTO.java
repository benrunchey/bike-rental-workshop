package io.axoniq.demo.bikerental.infra;

import java.util.List;

public record ProcessorStatusDTO(String name ,
                                 String tokenStoreIdentifier,
                                 Boolean running,
                                 Boolean error,
                                 Boolean resettable,
                                 Integer activeProcessorThreads,
                                 Integer availableProcessorThreads,
                                 Integer batchSize,
                                 String type,
                                 Boolean dlqConfigured,
                                 List<SegmentDTO> segments) {
}
