package com.smartass.server.simulator;

import com.smartass.server.kafka.KafkaDeviceDataProducerService;
import com.smartass.server.model.device.TemperatureSensorData;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;

@Component
@Profile("simulator")
public class TemperatureSensorSimulator implements Simulator {

    private final KafkaDeviceDataProducerService kafkaProducerService;
    private final Random random = new Random();
    private double baseTemperature = 20.0;
    private double temperatureDrift = 0.02;
    private double anomalyChance = 0.1;
    private boolean windowOpen = false;
    private boolean heatingUp = false;
    private boolean temperatureSpike = false;
    private final double criticalTemperature = 23.0;
    private double simulatedTemperature = baseTemperature;

    public TemperatureSensorSimulator(KafkaDeviceDataProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @Override
    public void simulate() {
        Flux.interval(Duration.ofSeconds(15))
                .flatMap(tick -> {
                    long timestamp = Instant.now().toEpochMilli();

                    double hourFactor = (timestamp / (1000.0 * 60 * 60)) % 24;
                    double temperatureVariation = Math.sin(hourFactor / 24 * Math.PI * 2-Math.PI/2) * 2;
                    double targetTemperature = baseTemperature + (Math.random() - 0.5) * temperatureDrift + temperatureVariation;

                    if (Math.random() < anomalyChance && !temperatureSpike && !windowOpen && !heatingUp) {
                        temperatureSpike = true;
                    }

                    if (temperatureSpike) {
                        simulatedTemperature += 0.5 + (Math.random() - 0.5) / 10;
                        if (simulatedTemperature >= criticalTemperature) {
                            temperatureSpike = false;
                        }
                    }
                    else if (windowOpen) {
                        simulatedTemperature -= 0.05 + (Math.random() - 0.5) / 100;
                        if (simulatedTemperature <= targetTemperature - 1) {
                            windowOpen = false;
                            heatingUp = true;
                        }
                    }
                    else if (heatingUp) {
                        simulatedTemperature += 0.025 + (Math.random() - 0.5) / 100;
                        if (simulatedTemperature >= targetTemperature) {
                            heatingUp = false;
                        }
                    }
                    else {
                        simulatedTemperature = targetTemperature;
                    }

                    double simulatedHumidity = (55*baseTemperature)/(6.1078 * Math.pow(10, (7.5 * simulatedTemperature) / (237.3 + simulatedTemperature)))+Math.random();

                    TemperatureSensorData data = TemperatureSensorData.builder()
                            .deviceId("sensor-001")
                            .type("temperature")
                            .timestamp(timestamp)
                            .temperature(simulatedTemperature)
                            .humidity(simulatedHumidity)
                            .authKey("key123")
                            .build();

                    return kafkaProducerService.send(data);
                })
                .subscribe();
    }
}
