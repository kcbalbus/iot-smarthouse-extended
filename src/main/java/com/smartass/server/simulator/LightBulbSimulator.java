package com.smartass.server.simulator;

import com.smartass.server.kafka.KafkaDeviceDataProducerService;
import com.smartass.server.model.command.DeviceCommandDTO;
import com.smartass.server.model.device.LightBulbData;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Profile("simulator")
public class LightBulbSimulator implements Simulator, ActuatorUpdatable {

    private final KafkaDeviceDataProducerService kafkaProducerService;
    private final SimulatorRegistry simulatorRegistry;
    private final AtomicBoolean isOn = new AtomicBoolean(false);
    private int activeTime = 0;
    private boolean motionDetected = false; // Flaga wykrycia ruchu
    private int motionDuration = 0; // Czas trwania ruchu
    private final int MAX_MOTION_DURATION = 20;

    public LightBulbSimulator(KafkaDeviceDataProducerService kafkaProducerService, SimulatorRegistry simulatorRegistry) {
        this.kafkaProducerService = kafkaProducerService;
        this.simulatorRegistry = simulatorRegistry;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        simulatorRegistry.register("light-001", this);
    }

    @Override
    public void simulate() {
        // --- Previous randomized simulation (commented out for tests) ---
        /*
        Flux.interval(Duration.ofSeconds(10))
                .flatMap(tick -> {
                    long timestamp = Instant.now().toEpochMilli();
                    int hour = (int) ((timestamp / (1000 * 60 * 60)) % 24);
                    boolean isNighttime = (hour >= 18 || hour < 6);
                    double turnOnProbability = getTurnOnProbability(hour);

                    if (motionDetected) {
                        isOn.set(true); // Zapal światło po wykryciu ruchu
                        motionDuration++;
                        if (motionDuration >= MAX_MOTION_DURATION) {
                            motionDetected = false; // Zakończ zdarzenie ruchu
                            isOn.set(false); // Wyłącz światło
                            motionDuration = 0;
                        }
                    }
                    else if (!isOn.get()) {
                        if (ThreadLocalRandom.current().nextDouble() < turnOnProbability) {
                            isOn.set(true);
                            activeTime = 0;
                        }
                    }
                    else {
                        activeTime++;

                        boolean shouldTurnOff =
                                activeTime > (isNighttime ? 50 : 30) ||
                                        (!isNighttime && ThreadLocalRandom.current().nextDouble() < 0.4);

                        if (shouldTurnOff) {
                            isOn.set(false);
                            activeTime = 0;
                        }
                    }

                    int brightness = 0;

                    if (isOn.get()) {
                        brightness = 90 + ThreadLocalRandom.current().nextInt(11);
                        brightness = Math.min(100, brightness);

                        if (ThreadLocalRandom.current().nextDouble() < 0.01) {
                            brightness = ThreadLocalRandom.current().nextInt(20, 50);
                        }
                    }

                    LightBulbData data = LightBulbData.builder()
                            .deviceId("light-001")
                            .type("light")
                            .timestamp(timestamp)
                            .state(isOn.get())
                            .brightness(brightness)
                            .authKey("key456")
                            .build();

                    return kafkaProducerService.send(data);
                })
                .subscribe();
        */

        // --- Simplified simulation for tests: telemetry reflects current isOn state,
        //     but brightness is computed using previous brightness logic when isOn=true ---
        Flux.interval(Duration.ofSeconds(10))
                .flatMap(tick -> {
                    long timestamp = Instant.now().toEpochMilli();

                    int brightness = 0;

                    if (isOn.get()) {
                        // use original brightness calculation when lamp is ON
                        brightness = 90 + ThreadLocalRandom.current().nextInt(11);
                        brightness = Math.min(100, brightness);

                        // occasional dimming event
                        if (ThreadLocalRandom.current().nextDouble() < 0.05) {
                            brightness = ThreadLocalRandom.current().nextInt(20, 50);
                        }
                    } else {
                        brightness = 0;
                    }

                    LightBulbData data = LightBulbData.builder()
                            .deviceId("light-001")
                            .type("light")
                            .timestamp(timestamp)
                            .state(isOn.get())
                            .brightness(brightness)
                            .authKey("key456")
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

        if ("set".equalsIgnoreCase(cmd) || "on".equalsIgnoreCase(cmd) || "setOn".equalsIgnoreCase(cmd)) {
            if (value != null && ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value))) {
                boolean v = Boolean.parseBoolean(value);
                isOn.set(v);

                return true;
            }
        }
        return false;
    }

    private double getTurnOnProbability(int hour) {
        if (hour >= 18 || hour < 6) return 0.7;
        if (hour >= 12 && hour < 14) return 0.3;
        if (hour >= 8 && hour < 10) return 0.2;
        return 0.1;
    }
}
