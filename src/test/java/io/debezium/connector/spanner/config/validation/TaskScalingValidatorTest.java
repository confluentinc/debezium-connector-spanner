/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.config.Configuration;

class TaskScalingValidatorTest {

    private static Stream<Arguments> configProvider() {
        return Stream.of(
                // autoscaling enabled, max > min → valid
                Arguments.of(
                        Configuration.from(Map.of(
                                "scaler.monitor.enabled", "true",
                                "tasks.max", "10",
                                "tasks.min", "2")),
                        true),
                // autoscaling enabled, max == min → valid
                Arguments.of(
                        Configuration.from(Map.of(
                                "scaler.monitor.enabled", "true",
                                "tasks.max", "5",
                                "tasks.min", "5")),
                        true),
                // autoscaling enabled, max < min → invalid
                Arguments.of(
                        Configuration.from(Map.of(
                                "scaler.monitor.enabled", "true",
                                "tasks.max", "1",
                                "tasks.min", "5")),
                        false),
                // autoscaling disabled, max < min → valid (no check needed)
                Arguments.of(
                        Configuration.from(Map.of(
                                "scaler.monitor.enabled", "false",
                                "tasks.max", "1",
                                "tasks.min", "5")),
                        true),
                // autoscaling not set (defaults to false), max < min → valid
                Arguments.of(
                        Configuration.from(Map.of(
                                "tasks.max", "1",
                                "tasks.min", "5")),
                        true));
    }

    @ParameterizedTest
    @MethodSource("configProvider")
    void validate(Configuration configuration, boolean isSuccess) {
        Map<String, ConfigValue> configValueMap = new HashMap<>();
        configValueMap.put("tasks.max", new ConfigValue("tasks.max"));
        configValueMap.put("tasks.min", new ConfigValue("tasks.min"));
        configValueMap.put("scaler.monitor.enabled", new ConfigValue("scaler.monitor.enabled"));

        ConfigurationValidator.ValidationContext validationContext = new ConfigurationValidator.ValidationContext(configuration, configValueMap);

        TaskScalingValidator taskScalingValidator = TaskScalingValidator.withContext(validationContext);
        taskScalingValidator.validate();
        Assertions.assertEquals(isSuccess, taskScalingValidator.isSuccess());
    }
}