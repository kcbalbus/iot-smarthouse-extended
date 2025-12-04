package com.smartass.server.model.scenario;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Scenario {
    private String id;
    private String name;
    private String triggerType; // e.g. "smoke", "motion"
    private List<ScenarioAction> actions;
}

