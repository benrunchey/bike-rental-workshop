package io.axoniq.demo.bikerental.infra;

import java.util.List;

public record TokenInformationDTO(String nodeId,
                                  Long headIndex,
                                  List<ProcessorDTO> processors) {
}
