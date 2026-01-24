package com.smartass.server.simulator;

import com.smartass.server.kafka.KafkaDeviceDataProducerService;
import com.smartass.server.model.command.DeviceCommandDTO;
import com.smartass.server.model.device.FridgeTemperatureSensorData;
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
public class FridgeTemperatureSensorSimulator implements Simulator, ActuatorUpdatable {

    private final KafkaDeviceDataProducerService kafkaProducerService;
    private final Random random = new Random();

    private final double targetTemperature = 6.0;
    private final double temperatureDrift = 0.001;
    private final double doorOpenChance = 0.01;
    private final double powerOutageChance = 0.00005;

    private final AtomicBoolean doorOpen = new AtomicBoolean(false);
    private int doorOpenTime = 0;
    private boolean powerOutage = false;
    private int powerOutageTime = 0;
    private double simulatedTemperature = targetTemperature;
    private boolean cooling = false;

    private static final double DOOR_OPEN_TEMP_INCREASE = 0.2;
    private static final double POWER_OUTAGE_TEMP_INCREASE = 0.05;
    private static final double COOLING_RATE = 0.1;

    private int MAX_DOOR_OPEN_TIME = 10;
    private int MAX_POWER_OUTAGE_TIME = 40;
    private final SimulatorRegistry simulatorRegistry;

    public FridgeTemperatureSensorSimulator(KafkaDeviceDataProducerService kafkaProducerService, SimulatorRegistry simulatorRegistry) {
        this.kafkaProducerService = kafkaProducerService;
        this.simulatorRegistry = simulatorRegistry;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        simulatorRegistry.register("fridge-001", this);
    }

    @Override
    public void simulate() {
        Flux.interval(Duration.ofSeconds(15))
                .flatMap(tick -> {
                    long timestamp = Instant.now().toEpochMilli();

                    double compressorCycle = (timestamp / (1000.0 * 60 * 20)) % 1;
                    double temperatureVariation = Math.sin(compressorCycle * Math.PI * 2);

                    if (!powerOutage && !doorOpen.get() && !cooling) {
                        if (random.nextDouble() < doorOpenChance) {
                            doorOpen.set(true);
                            doorOpenTime = 0;
                            MAX_DOOR_OPEN_TIME = MAX_DOOR_OPEN_TIME + random.nextInt(MAX_DOOR_OPEN_TIME);
                        }
                        else if (random.nextDouble() < powerOutageChance) {
                            powerOutage = true;
                            powerOutageTime = 0;
                            MAX_POWER_OUTAGE_TIME = MAX_POWER_OUTAGE_TIME + random.nextInt(MAX_POWER_OUTAGE_TIME);
                        }
                        else {
                            simulatedTemperature = targetTemperature + (random.nextDouble() - 0.5) * temperatureDrift + temperatureVariation;
                        }
                    }

                    else if (doorOpen.get()) {
                        simulatedTemperature += DOOR_OPEN_TEMP_INCREASE + (random.nextDouble() - 0.5) * 0.05;
                        doorOpenTime++;

                        if (doorOpenTime >= MAX_DOOR_OPEN_TIME || random.nextDouble() < 0.1) {
                            doorOpen.set(false);
                            cooling = true;
                        }
                    }

                    else if (powerOutage) {
                        simulatedTemperature += POWER_OUTAGE_TEMP_INCREASE + (random.nextDouble() - 0.5) * 0.01;
                        powerOutageTime++;

                        if (powerOutageTime >= MAX_POWER_OUTAGE_TIME || random.nextDouble() < 0.02) {
                            powerOutage = false;
                            cooling = true;
                        }
                    }

                    if (cooling) {
                        simulatedTemperature = simulatedTemperature - COOLING_RATE + (random.nextDouble() - 0.5)*0.05;
                        if (simulatedTemperature < targetTemperature + temperatureVariation) {
                            cooling = false;
                        }
                    }


                    FridgeTemperatureSensorData data = FridgeTemperatureSensorData.builder()
                            .deviceId("fridge-001")
                            .type("fridge")
                            .timestamp(timestamp)
                            .temperature(simulatedTemperature)
                            .doorOpen(doorOpen.get())
                            .authKey("key997")
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

        if ("set".equalsIgnoreCase(cmd) || "door".equalsIgnoreCase(cmd) || "setDoor".equalsIgnoreCase(cmd)) {
            if (value != null && ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value))) {
                boolean v = Boolean.parseBoolean(value);
                doorOpen.set(v);
                // emit immediate telemetry
                FridgeTemperatureSensorData data = FridgeTemperatureSensorData.builder()
                        .deviceId("fridge-001")
                        .type("fridge")
                        .timestamp(Instant.now().toEpochMilli())
                        .temperature(simulatedTemperature)
                        .doorOpen(doorOpen.get())
                        .authKey("key997")
                        .build();
                kafkaProducerService.send(data).subscribe();
                return true;
            }
        }
        return false;
    }
}