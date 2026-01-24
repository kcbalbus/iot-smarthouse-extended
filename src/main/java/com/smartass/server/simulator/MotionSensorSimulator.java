package com.smartass.server.simulator;

import com.smartass.server.kafka.KafkaDeviceDataProducerService;
import com.smartass.server.model.command.DeviceCommandDTO;
import com.smartass.server.model.device.MotionSensorData;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Profile("simulator")
public class MotionSensorSimulator implements Simulator, ActuatorUpdatable {

    private final KafkaDeviceDataProducerService kafkaProducerService;
    private final Random random = new Random();
    private int burglaryDuration = 0;
    private final double burglaryChance = 0.0001;
    private boolean burglaryDetected = false;
    private int maxburglaryDuration = 0;
    private final AtomicBoolean forcedMotion = new AtomicBoolean(false);
    private final SimulatorRegistry simulatorRegistry;

    public MotionSensorSimulator(KafkaDeviceDataProducerService kafkaProducerService, SimulatorRegistry simulatorRegistry) {
        this.kafkaProducerService = kafkaProducerService;
        this.simulatorRegistry = simulatorRegistry;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        simulatorRegistry.register("motion-001", this);
    }

    @Override
    public void simulate() {
        Flux.interval(Duration.ofSeconds(5))
                .flatMap(tick -> {
                    long timestamp = Instant.now().toEpochMilli();
                    LocalTime now = LocalTime.now();
                    boolean motionDetected = false;

                    if (forcedMotion.get()) {
                        motionDetected = true;
                    } else {
                        double lambda;
                        if (now.isAfter(LocalTime.of(6, 0)) && now.isBefore(LocalTime.of(7, 0))) {
                            lambda = 0.09;
                        }
                        else if (now.isAfter(LocalTime.of(7, 0)) && now.isBefore(LocalTime.of(14, 0))){
                            lambda = 0.005;
                        }
                        else if (now.isAfter(LocalTime.of(14, 0)) && now.isBefore(LocalTime.of(23, 0))){
                            lambda = 0.07;
                        }
                        else {
                            lambda = 0.0001;
                            if (!burglaryDetected && Math.random() < burglaryChance) {
                                burglaryDetected = true;
                                burglaryDuration = 0;
                                maxburglaryDuration = 12*20 + random.nextInt(120);
                            }
                            else if (burglaryDetected && burglaryDuration < maxburglaryDuration) {
                                burglaryDuration++;
                                lambda = 0.1;
                            }
                        }

                        double p = random.nextDouble();
                        if (p < lambda) {
                            motionDetected = true;
                        }
                    }

                    MotionSensorData data = MotionSensorData.builder()
                            .deviceId("motion-001")
                            .type("motion")
                            .timestamp(timestamp)
                            .motionDetected(motionDetected)
                            .authKey("key745")
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
        if (cmd == null) return false;

        if ("set".equalsIgnoreCase(cmd) || "motion".equalsIgnoreCase(cmd) || "setMotion".equalsIgnoreCase(cmd)) {
            if (value != null && ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value))) {
                boolean v = Boolean.parseBoolean(value);
                forcedMotion.set(v);
                // emit immediate telemetry

                return true;
            }
        }
        return false;
    }
}