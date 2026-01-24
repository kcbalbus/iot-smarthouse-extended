package com.smartass.server.controller;

import com.smartass.server.model.scenario.Scenario;
import com.smartass.server.registry.ScenariosRegistry;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequestMapping("/scenario")
public class ScenarioController {

    private final ScenariosRegistry scenariosRegistry;

    public ScenarioController(ScenariosRegistry scenariosRegistry) {
        this.scenariosRegistry = scenariosRegistry;
    }

    @GetMapping
    public Flux<Scenario> getAllScenarios() {
        return Flux.fromIterable(scenariosRegistry.getAllScenarios());
    }

    @GetMapping("/{id}")
    public Mono<Scenario> getScenario(@PathVariable String id) {
        return Mono.justOrEmpty(scenariosRegistry.findById(id));
    }

    @PutMapping("/{id}/enabled")
    public Mono<?> setEnabled(@PathVariable String id, @RequestBody Map<String, Boolean> body) {
        Boolean enabled = body != null ? body.get("enabled") : null;
        if (enabled == null) return Mono.fromCallable(() -> Map.of("error", "missing 'enabled' field"));

        boolean ok = scenariosRegistry.setEnabled(id, enabled);
        if (!ok) return Mono.empty();
        return Mono.fromCallable(() -> Map.of("id", id, "enabled", enabled));
    }
}
