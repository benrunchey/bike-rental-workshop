package io.axoniq.demo.bikerental.infra;

import org.axonframework.config.Configuration;
import org.axonframework.config.EventProcessingConfiguration;
import org.axonframework.eventhandling.EventProcessor;
import org.axonframework.eventhandling.StreamingEventProcessor;
import org.axonframework.eventhandling.TrackingEventProcessor;
import org.axonframework.springboot.EventProcessorProperties;
import org.springframework.web.bind.annotation.*;
import org.axonframework.eventsourcing.eventstore.EventStore;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@RestController()
@RequestMapping("/eventprocessors")
public class EventProcessorController {

    private EventProcessingConfiguration processingConfiguration;
    private EventStore eventStore;
    private EventProcessorProperties eventProcessorProperties;

    public EventProcessorController(EventProcessingConfiguration processingConfiguration,
                                    EventStore eventStore,
                                    EventProcessorProperties eventProcessorProperties) {
        this.processingConfiguration = processingConfiguration;
        this.eventStore = eventStore;
        this.eventProcessorProperties = eventProcessorProperties;
    }

    @GetMapping()
    public List<ProcessorStatusDTO> getAllProcessors() {

        var headToken = this.eventStore.createHeadToken();

        final Long headPosition = (headToken != null)? headToken.position().orElse(0):0L;

       return this.processingConfiguration.eventProcessors().keySet().stream().map(
               name -> {
                   var deadLetterQueue = this.processingConfiguration.deadLetterQueue(name);
                   var eventProc = this.processingConfiguration.eventProcessor(name, StreamingEventProcessor.class).get();
                   var activeThreads = (eventProc instanceof TrackingEventProcessor) ? eventProc.processingStatus().size()  : 1;
                   var availableThreads = (eventProc instanceof TrackingEventProcessor) ? eventProc.maxCapacity()  : 1;
                   var props = this.eventProcessorProperties.getProcessors().get(name);

                   var segments = eventProc.processingStatus().values().stream().map(
                           status -> {
                                var currentIndex = status.getCurrentPosition().orElse(0);
                                var trackingToken = status.getTrackingToken();
                                return new SegmentDTO(
                                        (long) status.getSegment().getSegmentId(),
                                        status.getSegment().getMask() + 1,
                                        (trackingToken !=null)? trackingToken.getClass().getSimpleName(): "NOT SET",
                                        currentIndex,
                                        status.isReplaying(),
                                        (headPosition - currentIndex),
                                        status.getSegment().mergeableSegmentId(),
                                        status.getSegment().splitSegmentId()
                            );
                           }).toList();

                   return  new ProcessorStatusDTO(
                           name,
                           eventProc.getTokenStoreIdentifier(),
                           eventProc.isRunning(),
                           eventProc.isError(),
                           eventProc.supportsReset(),
                           activeThreads,
                           availableThreads,
                           (props !=null)? props.getBatchSize() : 1,
                           eventProc.getClass().getSimpleName(),
                           deadLetterQueue.isPresent(),
                           segments
                   );
               }).toList();
    }

    @PutMapping("/{eventProcessorName}/splitsegment/{segmentId}")
    public void splitSegment(@PathVariable("eventProcessorName") String eventProcessorName,@PathVariable("segmentId") int segmentId ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(streamingProcessor -> {
                    // Use the result to check whether the operation succeeded
                    CompletableFuture<Boolean> result =
                            streamingProcessor.splitSegment(segmentId);
                });
    }

    @PutMapping("/{eventProcessorName}/mergesegment/{segmentId}")
    public void mergeSegment(@PathVariable("eventProcessorName") String eventProcessorName,@PathVariable("segmentId") int segmentId ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(streamingProcessor -> {
                    // Use the result to check whether the operation succeeded
                    CompletableFuture<Boolean> result =
                            streamingProcessor.mergeSegment(segmentId);
                });
    }

    @PutMapping("/{eventProcessorName}/releasesegment/{segmentId}")
    public void startProcessor(@PathVariable("eventProcessorName") String eventProcessorName, @PathVariable("segmentId") int segmentId  ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(streamingProcessor -> {
                    streamingProcessor.releaseSegment(segmentId, 10, TimeUnit.SECONDS);
                });
    }
    @PutMapping("/{eventProcessorName}/shutdown")
    public void shutdownProcessor(@PathVariable("eventProcessorName") String eventProcessorName ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(EventProcessor::shutDown);
    }

    @PutMapping("/{eventProcessorName}/start")
    public void startProcessor(@PathVariable("eventProcessorName") String eventProcessorName ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(EventProcessor::start);
    }


}
