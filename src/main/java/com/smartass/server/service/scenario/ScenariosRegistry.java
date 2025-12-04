package com.smartass.server.service.scenario;

import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class ScenariosRegistry {

    private final List<Scenario> scenarios = new ArrayList<>();

    @PostConstruct
    public void init() {
        ScenarioAction turnOffLight = ScenarioAction.builder()
                .deviceTypePattern("light-*")
                .command("set")
                .value("false")
                .build();

        ScenarioAction openWindow = ScenarioAction.builder()
                .deviceTypePattern("window-actuator-*")
                .command("open")
                .value("true")
                .build();

        Scenario smokeScenario = Scenario.builder()
                .id("sc-1")
                .name("Smoke detected - open window & turn off lights")
                .triggerType("smoke")
                .actions(List.of(turnOffLight, openWindow))
                .build();

        scenarios.add(smokeScenario);
    }

    public List<Scenario> findByTriggerType(String triggerType) {
        return scenarios.stream()
                .filter(s -> s.getTriggerType() != null && s.getTriggerType().equalsIgnoreCase(triggerType))
                .collect(Collectors.toList());
    }
}
