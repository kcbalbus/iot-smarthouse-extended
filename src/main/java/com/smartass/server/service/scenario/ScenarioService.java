package com.smartass.server.service.scenario;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.smartass.server.model.alert.AlertDTO;
import com.smartass.server.model.command.DeviceCommandDTO;
import com.smartass.server.model.device.DeviceData;
import com.smartass.server.registry.ScenariosRegistry;
import com.smartass.server.service.dispatch.DeviceCommandDispatcher;
import com.smartass.server.websocket.DeviceTelemetryWebSocketHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

@Service
public class ScenarioService {
    private static final Logger log = LoggerFactory.getLogger(ScenarioService.class);

    private final ScenariosRegistry registry;
    private final DeviceCommandDispatcher dispatcher;
    private final DeviceTelemetryWebSocketHandler telemetryWebSocketHandler;
    private final ObjectMapper objectMapper;

    public ScenarioService(ScenariosRegistry registry, DeviceCommandDispatcher dispatcher, DeviceTelemetryWebSocketHandler telemetryWebSocketHandler, ObjectMapper objectMapper) {
        this.registry = registry;
        this.dispatcher = dispatcher;
        this.telemetryWebSocketHandler = telemetryWebSocketHandler;
        this.objectMapper = objectMapper;
    }

    public void executeForAlerts(List<AlertDTO> alerts, DeviceData triggeringData) {
        if (alerts == null || alerts.isEmpty()) return;

        log.info("Executing scenarios for {} alerts, triggering device={}", alerts.size(), triggeringData != null ? triggeringData.getDeviceId() : "<null>");

        List<DeviceCommandDTO> commandsToSend = new ArrayList<>();

        for (AlertDTO alert : alerts) {
            String type = null;
            try {
                if (alert != null) type = alert.getType();
            } catch (Throwable ignored) {
                // defensive: if AlertDTO doesn't expose getType(), fallback to device type
            }

            if (type == null && triggeringData != null) {
                type = triggeringData.getType();
            }

            if (type == null) continue;

            log.info("Looking up scenarios for trigger type='{}' (alert={})", type, alert != null ? alert.getDescription() : "<none>");

            registry.findByTriggerType(type).forEach(scenario -> {
                // skip disabled scenarios
                try {
                    if (!scenario.isEnabled()) {
                        log.debug("Skipping disabled scenario '{}'", scenario.getId());
                        return;
                    }
                } catch (Exception ex) {
                    // if scenario doesn't have enabled flag for any reason, continue (backwards compatibility)
                }

                if (scenario.getTriggerConditionId() != null && alert != null) {
                    if (alert.getConditionId() == null || !alert.getConditionId().equals(scenario.getTriggerConditionId())) {
                        log.debug("Skipping scenario '{}' because triggerConditionId '{}' doesn't match alert.conditionId '{}'", scenario.getId(), scenario.getTriggerConditionId(), alert.getConditionId());
                        return;
                    }
                }

                log.info("Matched scenario '{}' : {} actions (scenarioTriggerType={})", scenario.getName(), scenario.getActions() != null ? scenario.getActions().size() : 0, scenario.getTriggerType());

                try {
                    long now = System.currentTimeMillis();
                    ObjectNode scenarioNode = objectMapper.valueToTree(scenario);
                    scenarioNode.put("timestamp", now);

                    String scenarioJson = objectMapper.writeValueAsString(scenarioNode);
                    telemetryWebSocketHandler.broadcastScenario(scenarioJson);
                } catch (Exception e) {
                    log.warn("Failed to broadcast scenario {}", scenario.getId(), e);
                }

                if (scenario.getActions() == null) return;
                scenario.getActions().forEach(action -> {
                    String targetDeviceId = action.getDeviceId();

                    if (targetDeviceId == null) {
                        log.warn("Scenario action has no target device specified: {}", action);
                        return;
                    }

                    DeviceCommandDTO cmd = DeviceCommandDTO.builder()
                            .deviceId(targetDeviceId)
                            .command(action.getCommand())
                            .value(action.getValue())
                            .build();

                    log.info("Prepared command for scenario: deviceId={}, command={}, value={}", cmd.getDeviceId(), cmd.getCommand(), cmd.getValue());

                    commandsToSend.add(cmd);
                });
            });
        }

        // send commands asynchronously
        Flux.fromIterable(commandsToSend)
                .flatMap(cmd -> dispatcher.dispatchCommand(cmd).onErrorResume(e -> {
                    log.error("Failed to dispatch command {} for scenario", cmd, e);
                    return reactor.core.publisher.Mono.empty();
                }))
                .subscribe();
    }
}
