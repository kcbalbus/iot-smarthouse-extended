package com.smartass.server.simulator;

import com.smartass.server.kafka.KafkaDeviceDataProducerService;
import com.smartass.server.model.device.WindowSensorData;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;

@Component
@Profile("simulator")
public class WindowSimulator implements Simulator {

    private final KafkaDeviceDataProducerService kafkaProducerService;
    private boolean isOpen = false;

    public WindowSimulator(KafkaDeviceDataProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @Override
    public void simulate() {
        // toggle window state periodically
        Flux.interval(Duration.ofSeconds(15))
                .flatMap(tick -> {
                    long timestamp = Instant.now().toEpochMilli();

                    // flip state every tick (simple simulation)
                    isOpen = !isOpen;

                    WindowSensorData data = WindowSensorData.builder()
                            .deviceId("window-001")
                            .type("window")
                            .timestamp(timestamp)
                            .isOpen(isOpen)
                            .authKey("key901")
                            .build();

                    return kafkaProducerService.send(data);
                })
                .subscribe();
    }
}
