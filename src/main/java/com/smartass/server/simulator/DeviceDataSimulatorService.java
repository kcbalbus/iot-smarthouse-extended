package com.smartass.server.simulator;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Profile("simulator")
public class DeviceDataSimulatorService {

    private final SimulatorProperties simulatorProperties;
    private final List<Simulator> simulators;

    public DeviceDataSimulatorService(SimulatorProperties simulatorProperties,
                                      List<Simulator> simulators) {
        this.simulatorProperties = simulatorProperties;
        this.simulators = simulators;
        startEnabledSimulations();
    }
    @PostConstruct
    private void startEnabledSimulations() {
        for (Simulator simulator : simulators) {
            if (isEnabled(simulator)) {
                simulator.simulate();
            }
        }
    }

    private boolean isEnabled(Simulator simulator) {
        return (simulator instanceof LightBulbSimulator && simulatorProperties.getLightbulb().isEnabled()) ||
                (simulator instanceof TemperatureSensorSimulator && simulatorProperties.getTemperature().isEnabled()) ||
                (simulator instanceof SmokeDetectorSimulator && simulatorProperties.getSmoke().isEnabled()) ||
                (simulator instanceof EnergyMeterSimulator && simulatorProperties.getEnergy().isEnabled()) ||
                (simulator instanceof MotionSensorSimulator && simulatorProperties.getMotion().isEnabled()) ||
                (simulator instanceof FridgeTemperatureSensorSimulator && simulatorProperties.getFridge().isEnabled()) ||
                (simulator instanceof WindowSimulator && simulatorProperties.getWindow().isEnabled());
    }
}
