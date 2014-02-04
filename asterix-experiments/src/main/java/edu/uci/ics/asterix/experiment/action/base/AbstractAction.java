package edu.uci.ics.asterix.experiment.action.base;

public abstract class AbstractAction implements IAction {

    private final IExceptionListener el;

    protected AbstractAction() {
        el = new DefaultExceptionListener();
    }

    protected AbstractAction(IExceptionListener el) {
        this.el = el;
    }

    @Override
    public void perform() {
        try {
            doPerform();
        } catch (Throwable t) {
            el.caughtException(t);
        }
    }

    protected abstract void doPerform() throws Exception;

}
