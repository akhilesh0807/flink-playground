package com.example.flink.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TickOutRecord(
    @JsonProperty("avg") double avg,
    @JsonProperty("startDate") long startDate,
    @JsonProperty("endDate") long endDate
) {
}
