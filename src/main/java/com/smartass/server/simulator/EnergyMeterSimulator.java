package com.smartass.server.simulator;

import com.smartass.server.kafka.KafkaDeviceDataProducerService;
import com.smartass.server.model.command.DeviceCommandDTO;
import com.smartass.server.model.device.EnergyMeterData;
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
public class EnergyMeterSimulator implements Simulator, ActuatorUpdatable {

    private final KafkaDeviceDataProducerService kafkaProducerService;
    private final Random random = new Random();
    private double currentPower;
    private double totalEnergy;
    private int currentState;
    private int stateDuration = 0;
    private final AtomicBoolean isSocketOn = new AtomicBoolean(true);
    private int reconnectDelay = 0;
    private int reconnectCounter = 0;
    private final SimulatorRegistry simulatorRegistry;

    public EnergyMeterSimulator(KafkaDeviceDataProducerService kafkaProducerService, SimulatorRegistry simulatorRegistry) {
        this.kafkaProducerService = kafkaProducerService;
        this.currentPower = 20.0 + random.nextDouble() * 10;
        this.totalEnergy = 0.0;
        this.simulatorRegistry = simulatorRegistry;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        simulatorRegistry.register("energy-001", this);
    }

    @Override
    public void simulate() {
        Flux.interval(Duration.ofSeconds(5))
                .flatMap(tick -> {
                    long timestamp = Instant.now().toEpochMilli();
                    if (!isSocketOn.get()) {
                        if (reconnectCounter > 0) {
                            reconnectCounter--;
                        }
                        else {
                            isSocketOn.set(true);
                        }
                    }

                    if (isSocketOn.get()) {
                        stateDuration++;
                        if (stateDuration >= 60 + random.nextInt(60)) {
                            currentState = random.nextInt(4);
                            stateDuration = 0;
                        }

                        switch (currentState) {
                            case 0:
                                currentPower = 3.0 + random.nextDouble() * 2;
                                break;
                            case 1:
                                currentPower = 50.0 + random.nextDouble() * 50;
                                break;
                            case 2:
                                currentPower = 100.0 + random.nextDouble() * 50;
                                break;
                            case 3:
                                currentPower = 200.0 + random.nextDouble() * 200;
                                break;
                        }
                    }
                    else {
                        currentPower = 0.0;
                        if (!isSocketOn.get() && reconnectCounter == 0) {
                            reconnectDelay = 5 + random.nextInt(10); // Losowy czas 5-15 sekund
                            reconnectCounter = reconnectDelay;
                        }

                    }

                    totalEnergy += (currentPower / 1000) * (5.0 / 3600);

                    EnergyMeterData data = EnergyMeterData.builder()
                            .deviceId("energy-001")
                            .type("energy")
                            .timestamp(timestamp)
                            .currentPower(currentPower)
                            .totalEnergy(totalEnergy)
                            .authKey("key412")
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

        if ("set".equalsIgnoreCase(cmd) || "on".equalsIgnoreCase(cmd) || "socket".equalsIgnoreCase(cmd)) {
            if (value != null && ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value))) {
                boolean v = Boolean.parseBoolean(value);
                isSocketOn.set(v);
                return true;
            }
        }
        return false;
    }
}
