package com.smartass.server.simulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Profile("simulator")
public class SimulatorRegistry {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Map<String, ActuatorUpdatable> registry = new ConcurrentHashMap<>();

    public void register(String deviceId, ActuatorUpdatable sim) {
        if (deviceId == null || sim == null) return;
        registry.put(deviceId, sim);
        log.info("Registered simulator for device {}", deviceId);
    }

    public void unregister(String deviceId) {
        registry.remove(deviceId);
        log.info("Unregistered simulator for device {}", deviceId);
    }

    public Optional<ActuatorUpdatable> find(String deviceId) {
        if (deviceId == null) return Optional.empty();
        return Optional.ofNullable(registry.get(deviceId));
    }
}
