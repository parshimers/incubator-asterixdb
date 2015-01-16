package edu.uci.ics.asterix.experiment.builder;

public abstract class AbstractExperimentBuilder {
    private final String name;

    protected AbstractExperimentBuilder(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public final Experiment build() throws Exception {
        Experiment e = new Experiment(name);
        doBuild(e);
        return e;
    }

    protected abstract void doBuild(Experiment e) throws Exception;
}
