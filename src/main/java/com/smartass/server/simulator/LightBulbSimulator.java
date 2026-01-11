package com.smartass.server.simulator;

import com.smartass.server.kafka.KafkaDeviceDataProducerService;
import com.smartass.server.model.device.LightBulbData;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

@Component
@Profile("simulator")
public class LightBulbSimulator implements Simulator {

    private final KafkaDeviceDataProducerService kafkaProducerService;
    private boolean isOn = false;
    private int activeTime = 0;
    private boolean motionDetected = false; // Flaga wykrycia ruchu
    private int motionDuration = 0; // Czas trwania ruchu
    private final int MAX_MOTION_DURATION = 20;

    public LightBulbSimulator(KafkaDeviceDataProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @Override
    public void simulate() {
        Flux.interval(Duration.ofSeconds(10))
                .flatMap(tick -> {
                    long timestamp = Instant.now().toEpochMilli();
                    int hour = (int) ((timestamp / (1000 * 60 * 60)) % 24);
                    boolean isNighttime = (hour >= 18 || hour < 6);
                    double turnOnProbability = getTurnOnProbability(hour);

                    if (motionDetected) {
                        isOn = true; // Zapal światło po wykryciu ruchu
                        motionDuration++;
                        if (motionDuration >= MAX_MOTION_DURATION) {
                            motionDetected = false; // Zakończ zdarzenie ruchu
                            isOn = false; // Wyłącz światło
                            motionDuration = 0;
                        }
                    }
                    else if (!isOn) {
                        if (ThreadLocalRandom.current().nextDouble() < turnOnProbability) {
                            isOn = true;
                            activeTime = 0;
                        }
                    }
                    else {
                        activeTime++;

                        boolean shouldTurnOff =
                                activeTime > (isNighttime ? 50 : 30) ||
                                        (!isNighttime && ThreadLocalRandom.current().nextDouble() < 0.4);

                        if (shouldTurnOff) {
                            isOn = false;
                            activeTime = 0;
                        }
                    }

                    int brightness = 0;

                    if (isOn) {
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
                            .state(isOn)
                            .brightness(brightness)
                            .authKey("key456")
                            .build();

                    return kafkaProducerService.send(data);
                })
                .subscribe();
    }

    private double getTurnOnProbability(int hour) {
        if (hour >= 18 || hour < 6) return 0.7;
        if (hour >= 12 && hour < 14) return 0.3;
        if (hour >= 8 && hour < 10) return 0.2;
        return 0.1;
    }
}