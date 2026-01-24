package com.smartass.server.simulator;

import com.smartass.server.kafka.KafkaDeviceDataProducerService;
import com.smartass.server.model.device.SmokeDetectorData;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;

@Component
@Profile("simulator")
public class SmokeDetectorSimulator implements Simulator {

    private final KafkaDeviceDataProducerService kafkaProducerService;
    private final Random random = new Random();
    private boolean smokeDetected = false;
    private boolean cigaretteDetected = false;
    private boolean fireDetected = false;
    private double battery;
    private final double batteryDrain = 0.005;
    private int smokeEventDuration = 0;
    private double smokeLevel = 0.01;
    private final double cigaretteChance = 0.005;
    private final double fireChance = 0.0001;
    private double chance = 0.0;
    private int batteryEventDuration = 0;
    private int batteryEventDurationEND = 0;
    private int maxFireDuration = 120;
    private int maxCigaretteDuration = 30;
    private boolean isEventEnding = false;
    private boolean windowOpen = false;

    public SmokeDetectorSimulator(KafkaDeviceDataProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
        this.battery = 80.0 + (new Random().nextDouble() * 20);
    }

    @Override
    public void simulate() {
        Flux.interval(Duration.ofSeconds(30))
                .flatMap(tick -> {
                    long timestamp = Instant.now().toEpochMilli();

                    if (!smokeDetected && !fireDetected && !cigaretteDetected && !isEventEnding) {
                        chance = Math.random();
                        battery -= batteryDrain * (1 + random.nextDouble() / 10);
                        if (chance < fireChance) {
                            fireDetected = true;
                            smokeEventDuration = 0;
                            maxFireDuration = 30 + random.nextInt(30);
                        } else if (chance < cigaretteChance) {
                            cigaretteDetected = true;
                            smokeEventDuration = 0;
                            maxCigaretteDuration = 7 + random.nextInt(7);
                        } else {
                            smokeLevel = random.nextDouble() * 0.1;
                        }
                    } else {
                        smokeEventDuration++;

                        if (fireDetected) {
                            if (smokeEventDuration <= maxFireDuration) {
                                smokeLevel = Math.min(0.9 + random.nextDouble() * 0.1, smokeLevel + 0.07 + random.nextDouble() * 0.01);
                            } else if (smokeEventDuration > maxFireDuration && smokeLevel > 0.1) {
                                smokeLevel = Math.max(0.0 + random.nextDouble() * 0.1, smokeLevel - (0.03 + random.nextDouble() * 0.01));
                                isEventEnding = true;
                            } else {
                                fireDetected = false;
                                isEventEnding = false;
                                smokeLevel = random.nextDouble() * 0.1;
                            }
                        } else if (cigaretteDetected) {
                            if (smokeEventDuration <= maxCigaretteDuration) {
                                smokeLevel = Math.min(0.5 + random.nextDouble() * 0.1, smokeLevel + 0.05 + random.nextDouble() * 0.01);
                            } else if (smokeEventDuration > maxCigaretteDuration && smokeLevel > 0.1) {
                                smokeLevel = Math.max(0.0 + random.nextDouble() * 0.1, smokeLevel - (0.03 + random.nextDouble() * 0.01));
                                isEventEnding = true;
                            } else {
                                cigaretteDetected = false;
                                isEventEnding = false;
                                smokeLevel = random.nextDouble() * 0.1;
                            }
                        }

                        if (smokeLevel > 0.2) {
                            smokeDetected = true;
                            battery -= batteryDrain * (1 + random.nextDouble());
                            windowOpen = true; // Otwórz okno po wykryciu dymu
                        } else {
                            smokeDetected = false;
                            battery -= batteryDrain * (1 + random.nextDouble() / 10);
                            windowOpen = false; // Zamknij okno, gdy dym zniknie
                        }
                    }

                    if (windowOpen) {
                        smokeLevel = Math.max(0.0, smokeLevel - (0.02 + random.nextDouble() * 0.01)); // Powolne zmniejszanie poziomu dymu
                    }

                    if (battery <= 0) {
                        if (batteryEventDuration == 0) {
                            batteryEventDurationEND = 120 + random.nextInt(120);
                        }
                        smokeLevel = Double.NaN;
                        smokeDetected = false;
                        fireDetected = false;
                        cigaretteDetected = false;
                        isEventEnding = false;
                        battery = 0;
                        batteryEventDuration += 1;

                        if (batteryEventDuration >= batteryEventDurationEND) {
                            battery = 80.0 + (random.nextDouble() * 20);
                            batteryEventDuration = 0;
                            batteryEventDurationEND = 0;
                        }
                    }

                    SmokeDetectorData data = SmokeDetectorData.builder()
                            .deviceId("smoke-001")
                            .type("smoke")
                            .timestamp(timestamp)
                            .smokeLevel(smokeLevel)
                            .alarmActive(smokeDetected)
                            .batteryLevel(battery)
                            .authKey("key835")
                            .build();

                    return kafkaProducerService.send(data);
                })
                .subscribe();
    }
}