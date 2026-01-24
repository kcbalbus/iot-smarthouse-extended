package com.smartass.server.simulator;

import com.smartass.server.kafka.KafkaDeviceDataProducerService;
import com.smartass.server.model.command.DeviceCommandDTO;
import com.smartass.server.model.device.TemperatureSensorData;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Profile("simulator")
public class TemperatureSensorSimulator implements Simulator, ActuatorUpdatable {

    private final KafkaDeviceDataProducerService kafkaProducerService;
    private final Random random = new Random();
    private double baseTemperature = 20.0;
    private double temperatureDrift = 0.02;
    private double anomalyChance = 0.1;
    private final AtomicBoolean windowOpen = new AtomicBoolean(false);
    private final AtomicBoolean heatingUp = new AtomicBoolean(false);
    private boolean temperatureSpike = false;
    private final double criticalTemperature = 23.0;
    private double simulatedTemperature = baseTemperature;
    private final SimulatorRegistry simulatorRegistry;

    public TemperatureSensorSimulator(KafkaDeviceDataProducerService kafkaProducerService, SimulatorRegistry simulatorRegistry) {
        this.kafkaProducerService = kafkaProducerService;
        this.simulatorRegistry = simulatorRegistry;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        simulatorRegistry.register("sensor-001", this);
    }

    @Override
    public void simulate() {
        Flux.interval(Duration.ofSeconds(15))
                .flatMap(tick -> {
                    long timestamp = Instant.now().toEpochMilli();

                    double hourFactor = (timestamp / (1000.0 * 60 * 60)) % 24;
                    double temperatureVariation = Math.sin(hourFactor / 24 * Math.PI * 2-Math.PI/2) * 2;
                    double targetTemperature = baseTemperature + (Math.random() - 0.5) * temperatureDrift + temperatureVariation;

                    if (Math.random() < anomalyChance && !temperatureSpike && !windowOpen.get() && !heatingUp.get()) {
                        temperatureSpike = true;
                    }

                    if (temperatureSpike) {
                        simulatedTemperature += 0.5 + (Math.random() - 0.5) / 10;
                        if (simulatedTemperature >= criticalTemperature) {
                            temperatureSpike = false;
                        }
                    }
                    else if (windowOpen.get()) {
                        simulatedTemperature -= 0.05 + (Math.random() - 0.5) / 100;
                        if (simulatedTemperature <= targetTemperature - 1) {
                            windowOpen.set(false);
                            heatingUp.set(true);
                        }
                    }
                    else if (heatingUp.get()) {
                        simulatedTemperature += 0.025 + (Math.random() - 0.5) / 100;
                        if (simulatedTemperature >= targetTemperature) {
                            heatingUp.set(false);
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

    @Override
    public boolean applyCommand(DeviceCommandDTO command) {
        if (command == null) return false;
        String cmd = command.getCommand();
        String value = command.getValue();
        if (cmd == null) return false;

        boolean handled = false;
        try {
            if ("set".equalsIgnoreCase(cmd)) {
                if (value != null && value.toLowerCase().contains("window")) {
                    String[] parts = value.split("=");
                    if (parts.length == 2) {
                        boolean v = Boolean.parseBoolean(parts[1]);
                        windowOpen.set(v);
                        handled = true;
                    }
                }
            } else if ("window".equalsIgnoreCase(cmd) || "setWindow".equalsIgnoreCase(cmd)) {
                if (value != null && ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value))) {
                    windowOpen.set(Boolean.parseBoolean(value));
                    handled = true;
                }
            }

        } catch (Exception e) {
            return false;
        }
        return handled;
    }
}
