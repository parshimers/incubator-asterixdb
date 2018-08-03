package org.apache.hyracks.control.nc;

import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Order;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.logging.log4j.core.config.plugins.Plugin;

import java.net.URI;

public class NCLogConfigurationFactory extends ConfigurationFactory {
    private NCConfig config;

    public NCLogConfigurationFactory(NCConfig config) {
        this.config = config;
    }

    public Configuration createConfiguration(final String name, ConfigurationBuilder<BuiltConfiguration> builder) {
        String nodeId = config.getNodeId();
        String logDir = config.getLogDir();
        builder.setStatusLevel(Level.WARN);
        builder.setConfigurationName("RollingBuilder");
        // create a rolling file appender
        LayoutComponentBuilder defaultLayout = builder.newLayout("PatternLayout").addAttribute("pattern",
                "%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n");
        ComponentBuilder triggeringPolicy = builder.newComponent("Policies")
                .addComponent(builder.newComponent("CronTriggeringPolicy").addAttribute("schedule", "0 0 0 * * ?"))
                .addComponent(builder.newComponent("SizeBasedTriggeringPolicy").addAttribute("size", "50M"));
        AppenderComponentBuilder defaultRoll =
                builder.newAppender("default", "RollingFile").addAttribute("fileName", logDir + "nc-" + nodeId + ".log")
                        .addAttribute("filePattern", logDir + "nc-" + nodeId + "-%d{MM-dd-yy}.log.gz")
                        .add(defaultLayout).addComponent(triggeringPolicy);
        builder.add(defaultRoll);

        // create the new logger
        builder.add(builder.newRootLogger(Level.INFO).add(builder.newAppenderRef("default")));

        LayoutComponentBuilder accessLayout = builder.newLayout("PatternLayout").addAttribute("pattern", "%m%n");
        AppenderComponentBuilder accessRoll = builder.newAppender("access", "RollingFile")
                .addAttribute("fileName", logDir + "access-" + nodeId + ".log")
                .addAttribute("filePattern", logDir + "access-" + nodeId + "-%d{MM-dd-yy}.log.gz").add(accessLayout)
                .addComponent(triggeringPolicy);
        builder.add(accessRoll);
        builder.add(builder.newLogger("org.apache.hyracks.http.server", Level.INFO)
                .add(builder.newAppenderRef("access")).addAttribute("additivity", false));

        return builder.build();
    }

    @Override
    public Configuration getConfiguration(final LoggerContext loggerContext, final ConfigurationSource source) {
        return getConfiguration(loggerContext, source.toString(), null);
    }

    @Override
    public Configuration getConfiguration(final LoggerContext loggerContext, final String name,
            final URI configLocation) {
        ConfigurationBuilder<BuiltConfiguration> builder = newConfigurationBuilder();
        return createConfiguration(name, builder);
    }

    @Override
    protected String[] getSupportedTypes() {
        return new String[] { "*" };
    }

    @Override
    public String toString() {
        return "NCLogConfiguration";
    }
}
