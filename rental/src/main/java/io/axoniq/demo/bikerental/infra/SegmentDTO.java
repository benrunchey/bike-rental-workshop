package io.axoniq.demo.bikerental.infra;

public record SegmentDTO(
        Long segment,
        Integer oneOf,
        String tokenType,
        Long currentIndex,
        Boolean replaying,
        Long behind,
        Integer mergeableSegment,
        Integer splitSegment) {
}
