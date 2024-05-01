package io.axoniq.demo.bikerental.infra;

import java.util.List;

public record ProcessorDTO(String name,
                           Long currentIndex,
                           Boolean replaying,
                           List<SegmentDTO> segments) {
}
