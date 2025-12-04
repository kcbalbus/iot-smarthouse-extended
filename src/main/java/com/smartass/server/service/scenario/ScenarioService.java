package com.smartass.server.service.scenario;

import com.smartass.server.model.alert.AlertDTO;
import com.smartass.server.model.command.DeviceCommandDTO;
import com.smartass.server.model.device.DeviceData;
import com.smartass.server.service.dispatch.DeviceCommandDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Service
public class ScenarioService {
    private static final Logger log = LoggerFactory.getLogger(ScenarioService.class);

    private final ScenariosRegistry registry;
    private final DeviceCommandDispatcher dispatcher;

    public ScenarioService(ScenariosRegistry registry, DeviceCommandDispatcher dispatcher) {
        this.registry = registry;
        this.dispatcher = dispatcher;
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
                log.info("Matched scenario '{}': {} actions", scenario.getName(), scenario.getActions() != null ? scenario.getActions().size() : 0);
                if (scenario.getActions() == null) return;
                scenario.getActions().forEach(action -> {
                    String targetDeviceId = action.getDeviceId();

                    if (targetDeviceId == null && action.getDeviceTypePattern() != null) {
                        String pattern = action.getDeviceTypePattern();
                        if (pattern.endsWith("*")) {
                            String prefix = pattern.substring(0, pattern.length() - 1);
                            if (triggeringData != null && triggeringData.getDeviceId() != null && triggeringData.getDeviceId().startsWith(prefix)) {
                                targetDeviceId = triggeringData.getDeviceId();
                            } else {
                                targetDeviceId = prefix + "001";
                            }
                        } else {
                            // regex style
                            try {
                                Pattern p = Pattern.compile(pattern);
                                if (triggeringData != null && triggeringData.getDeviceId() != null && p.matcher(triggeringData.getDeviceId()).matches()) {
                                    targetDeviceId = triggeringData.getDeviceId();
                                } else {
                                    // fallback
                                    targetDeviceId = pattern;
                                }
                            } catch (Exception ex) {
                                targetDeviceId = pattern;
                            }
                        }
                    }

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
