package edu.uci.ics.asterix.experiment.action.derived;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

public class ManagixActions {

    private enum ManagixCommand {
        CONFIGURE("configure"),
        CREATE("create", "-n", "-c", "-a"),
        START("start", "-n"),
        STOP("stop", "-n"),
        DELETE("delete", "-n"),
        LOG("log", "-n", "-d"),
        SHUTDOWN("shutdown");

        private final String cmdFormat;

        private ManagixCommand(String name, String... options) {
            StringBuilder sb = new StringBuilder();
            sb.append(name).append(" ");
            if (options != null) {
                for (int i = 0; i < options.length; ++i) {
                    sb.append(options[i]).append(" ").append("{").append(i).append("}");
                    if (i != options.length - 1) {
                        sb.append(" ");
                    }
                }
            }
            cmdFormat = sb.toString();
        }

        public String getCommandFormat() {
            return cmdFormat;
        }
    }

    private static abstract class AbstractManagixCommandAction extends AbstractLocalExecutableAction {

        private static final String cmdFormat = "{0}/bin/managix {1}";

        private final String managixHomePath;

        private final String command;

        protected AbstractManagixCommandAction(String managixHomePath, String command) {
            this.managixHomePath = managixHomePath;
            this.command = command;
        }

        @Override
        protected String getCommand() {
            return MessageFormat.format(cmdFormat, managixHomePath, command);
        }

        @Override
        protected Map<String, String> getEnvironment() {
            Map<String, String> env = new HashMap<>();
            env.put("MANAGIX_HOME", managixHomePath);
            return env;
        }

    }

    public static class ConfigureAsterixManagixAction extends AbstractManagixCommandAction {

        public ConfigureAsterixManagixAction(String managixHomePath) {
            super(managixHomePath, MessageFormat.format(ManagixCommand.CONFIGURE.getCommandFormat(), ""));
        }

    }

    public static class CreateAsterixManagixAction extends AbstractManagixCommandAction {

        public CreateAsterixManagixAction(String managixHomePath, String asterixInstanceName,
                String clusterConfigFilePath, String asterixConfigFilePath) {
            super(managixHomePath, MessageFormat.format(ManagixCommand.CREATE.getCommandFormat(), asterixInstanceName,
                    clusterConfigFilePath, asterixConfigFilePath));
        }

    }

    public static class StartAsterixManagixAction extends AbstractManagixCommandAction {

        public StartAsterixManagixAction(String managixHomePath, String asterixInstanceName) {
            super(managixHomePath, MessageFormat.format(ManagixCommand.START.getCommandFormat(), asterixInstanceName));
        }

    }

    public static class StopAsterixManagixAction extends AbstractManagixCommandAction {

        public StopAsterixManagixAction(String managixHomePath, String asterixInstanceName) {
            super(managixHomePath, MessageFormat.format(ManagixCommand.STOP.getCommandFormat(), asterixInstanceName));
        }

    }

    public static class DeleteAsterixManagixAction extends AbstractManagixCommandAction {

        public DeleteAsterixManagixAction(String managixHomePath, String asterixInstanceName) {
            super(managixHomePath, MessageFormat.format(ManagixCommand.DELETE.getCommandFormat(), asterixInstanceName));
        }

    }

    public static class LogAsterixManagixAction extends AbstractManagixCommandAction {

        public LogAsterixManagixAction(String managixHomePath, String asterixInstanceName, String destinationDir) {
            super(managixHomePath, MessageFormat.format(ManagixCommand.LOG.getCommandFormat(), asterixInstanceName,
                    destinationDir));
        }

    }

    public static class ShutdownAsterixManagixAction extends AbstractManagixCommandAction {

        public ShutdownAsterixManagixAction(String managixHomePath) {
            super(managixHomePath, MessageFormat.format(ManagixCommand.SHUTDOWN.getCommandFormat(), ""));
        }

    }
}
