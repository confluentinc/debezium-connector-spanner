/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.MAX_TASKS;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.MIN_TASKS;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.SCALER_MONITOR_ENABLED;
import static org.slf4j.LoggerFactory.getLogger;

import org.slf4j.Logger;

/**
 * Validates that task scaling configuration properties are consistent:
 * tasks.max must be greater than or equal to tasks.min when autoscaling is enabled
 */
public class TaskScalingValidator implements ConfigurationValidator.Validator {

    private static final Logger LOGGER = getLogger(TaskScalingValidator.class);
    private final ConfigurationValidator.ValidationContext validationContext;
    private boolean result = true;

    public TaskScalingValidator(ConfigurationValidator.ValidationContext validationContext) {
        this.validationContext = validationContext;
    }

    public static TaskScalingValidator withContext(ConfigurationValidator.ValidationContext validationContext) {
        return new TaskScalingValidator(validationContext);
    }

    @Override
    public boolean isSuccess() {
        return result;
    }

    @Override
    public ConfigurationValidator.Validator validate() {
        if (!canValidate()) {
            result = false;
            return this;
        }

        boolean scalerEnabled = Boolean.parseBoolean(validationContext.getString(SCALER_MONITOR_ENABLED));
        if (!scalerEnabled) {
            return this;
        }

        int maxTasks = Integer.parseInt(validationContext.getString(MAX_TASKS));
        int minTasks = Integer.parseInt(validationContext.getString(MIN_TASKS));

        if (maxTasks < minTasks) {
            String msg = "tasks.max (" + maxTasks + ") must be greater than or equal to tasks.min (" + minTasks + ")";
            LOGGER.error(msg);
            validationContext.error(msg, MAX_TASKS, MIN_TASKS);
            result = false;
        }

        return this;
    }

    private boolean canValidate() {
        return validationContext.getErrors(MAX_TASKS).isEmpty() && validationContext.getErrors(MIN_TASKS).isEmpty();
    }

}