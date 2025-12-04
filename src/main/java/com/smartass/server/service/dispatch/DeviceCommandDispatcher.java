package com.smartass.server.service.dispatch;

import com.smartass.server.kafka.KafkaCommandProducerService;
import com.smartass.server.model.command.DeviceCommandDTO;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class DeviceCommandDispatcher {
    private static final Logger log = LoggerFactory.getLogger(DeviceCommandDispatcher.class);

    private final KafkaCommandProducerService kafkaProducer;
    private final MeterRegistry meterRegistry;

    public DeviceCommandDispatcher(final KafkaCommandProducerService kafkaProducer,
                                   final MeterRegistry meterRegistry) {
        this.kafkaProducer = kafkaProducer;
        this.meterRegistry = meterRegistry;
    }

    public Mono<Void> dispatchCommand(DeviceCommandDTO command) {
        if (command.getDeviceId() == null || command.getCommand() == null) {
            meterRegistry.counter("iot.command.dispatch.error", "reason", "invalid_command").increment();
            return Mono.error(new IllegalArgumentException("Invalid command: deviceId and command must not be null"));
        }

        log.info("Dispatching command to device via Kafka: deviceId={}, command={}, value={}", command.getDeviceId(), command.getCommand(), command.getValue());

        return kafkaProducer.send(command)
                .doOnSuccess(v -> {
                    meterRegistry.counter("iot.command.dispatch.success", "command", command.getCommand()).increment();
                    log.info("Command dispatched successfully: deviceId={}, command={}", command.getDeviceId(), command.getCommand());
                })
                .doOnError(e -> {
                    meterRegistry.counter("iot.command.dispatch.error", "reason", "producer_error", "command", command.getCommand()).increment();
                    log.error("Error dispatching command to deviceId={}: {}", command.getDeviceId(), e.getMessage());
                });
    }
}
