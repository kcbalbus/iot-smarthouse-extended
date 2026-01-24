package com.smartass.server.registry;

import com.smartass.server.model.scenario.Scenario;
import com.smartass.server.model.scenario.ScenarioAction;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class ScenariosRegistry {

    private final List<Scenario> scenarios = new ArrayList<>();

    @PostConstruct
    public void init() {
        //actions

        ScenarioAction turnOffLight = ScenarioAction.builder()
                .deviceId("light-actuator-001")
                .command("set")
                .value("false")
                .build();

        ScenarioAction turnOnLight = ScenarioAction.builder()
                .deviceId("light-actuator-001")
                .command("set")
                .value("true")
                .build();

        ScenarioAction openWindow = ScenarioAction.builder()
                .deviceId("window-actuator-001")
                .command("open")
                .value("true")
                .build();

        // --- existing smoke scenario ---

        Scenario smokeScenario = Scenario.builder()
                .id("sc-1")
                .name("Smoke detected - open window & turn off lights")
                .triggerType("smoke")
                .triggerConditionId("smoke-alarmActive")
                .actions(List.of(turnOffLight, openWindow))
                .build();

        scenarios.add(smokeScenario);

        // --- temperature: high -> open window  ---

        Scenario tempHighScenario = Scenario.builder()
                .id("sc-2")
                .name("High temperature - open window")
                .triggerType("temperature")
                .triggerConditionId("temperature-high")
                .actions(List.of(openWindow))
                .build();

        scenarios.add(tempHighScenario);

        // --- motion detected -> turn on lights ---


        Scenario motionScenario = Scenario.builder()
                .id("sc-4")
                .name("Motion detected - turn on lights")
                .triggerType("motion")
                .triggerConditionId("motion-motionDetected")
                .actions(List.of(turnOnLight))
                .build();

        scenarios.add(motionScenario);

        // --- energy high -> reduce load (turn off lights, set HVAC to eco) ---

        Scenario energyHighScenario = Scenario.builder()
                .id("sc-5")
                .name("High energy consumption - reduce load")
                .triggerType("energy")
                .triggerConditionId("energy-currentPower-high")
                .actions(List.of(turnOffLight))
                .build();

        scenarios.add(energyHighScenario);

    }

    public List<Scenario> findByTriggerType(String triggerType) {
        return scenarios.stream()
                .filter(s -> s.getTriggerType() != null && s.getTriggerType().equalsIgnoreCase(triggerType))
                .collect(Collectors.toList());
    }


    public List<Scenario> getAllScenarios() {
        return new ArrayList<>(scenarios);
    }

    public Optional<Scenario> findById(String id) {
        if (id == null) return Optional.empty();
        return scenarios.stream().filter(s -> id.equals(s.getId())).findFirst();
    }

    public boolean setEnabled(String id, boolean enabled) {
        Optional<Scenario> opt = findById(id);
        if (opt.isEmpty()) return false;
        Scenario s = opt.get();
        s.setEnabled(enabled);
        return true;
    }
}
