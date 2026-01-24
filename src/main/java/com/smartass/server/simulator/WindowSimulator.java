package com.smartass.server.simulator;

import com.smartass.server.kafka.KafkaDeviceDataProducerService;
import com.smartass.server.model.command.DeviceCommandDTO;
import com.smartass.server.model.device.WindowSensorData;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import reactor.core.publisher.Flux;

@Component
@Profile("simulator")
public class WindowSimulator implements Simulator, ActuatorUpdatable {

    private final KafkaDeviceDataProducerService kafkaProducerService;
    private final SimulatorRegistry simulatorRegistry;
    private final AtomicBoolean isOpen = new AtomicBoolean(false);
    private final Random random = new Random();

    public WindowSimulator(KafkaDeviceDataProducerService kafkaProducerService, SimulatorRegistry simulatorRegistry) {
        this.kafkaProducerService = kafkaProducerService;
        this.simulatorRegistry = simulatorRegistry;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        // register under device id used by telemetry
        simulatorRegistry.register("window-001", this);
    }

    @Override
    public void simulate() {
        Flux.interval(Duration.ofSeconds(30))
                .flatMap(tick -> {
                    long timestamp = Instant.now().toEpochMilli();


                    if (isOpen.get()) {
                        if (random.nextDouble() < 0.7) {
                            isOpen.set(false);
                        }
                    }

                    WindowSensorData data = WindowSensorData.builder()
                            .deviceId("window-001")
                            .type("window")
                            .timestamp(timestamp)
                            .isOpen(isOpen.get())
                            .authKey("key901")
                            .build();

                    return kafkaProducerService.send(data);
                })
                .subscribe();
    }

    @Override
    public boolean applyCommand(DeviceCommandDTO command) {
        if (command == null) return false;

        String cmd = command.getCommand();
        String value = command.getValue();

        // handle several forms: command 'set' with value 'open=true', or command 'open'/'setOpen' with value 'true'/'false'
        boolean handled = false;
        try {
            if (cmd == null) return false;
            if ("set".equalsIgnoreCase(cmd)) {
                // parse value like "open=true" or just "true"
                if (value != null && value.toLowerCase().contains("open")) {
                    String[] parts = value.split("=");
                    if (parts.length == 2) {
                        boolean v = Boolean.parseBoolean(parts[1]);
                        isOpen.set(v);
                        handled = true;
                    }
                } else if (value != null && ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value))) {
                    boolean v = Boolean.parseBoolean(value);
                    isOpen.set(v);
                    handled = true;
                }
            } else if ("open".equalsIgnoreCase(cmd) || "setOpen".equalsIgnoreCase(cmd)) {
                if (value != null && ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value))) {
                    boolean v = Boolean.parseBoolean(value);
                    isOpen.set(v);
                    handled = true;
                }
            }

            if (handled){
                long timestamp = Instant.now().toEpochMilli();

                WindowSensorData data = WindowSensorData.builder()
                        .deviceId("window-001")
                        .type("window")
                        .timestamp(timestamp)
                        .isOpen(isOpen.get())
                        .authKey("key901")
                        .build();
                kafkaProducerService.send(data).subscribe();
            }

        } catch (Exception e) {
            // ignore and report false
            return false;
        }
        return handled;
    }
}
