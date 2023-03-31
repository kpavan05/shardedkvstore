package dslabs.atmostonce;

import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.lang.reflect.InvocationTargetException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application>
        implements Application {
    @Getter @NonNull private final T application;

    //TODO: declare fields for your implementation

    @Override
    public AMOResult execute(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;

        //TODO: execute the command
        //Hints: remember to check whether the command is executed before and update records
        return null;
    }

    // copy constructor
    public AMOApplication(AMOApplication<Application> amoApplication)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Application application = amoApplication.application();
        this.application = (T) application.getClass().getConstructor(application.getClass()).newInstance(application);
        //TODO: a deepcopy constructor
        //Hints: remember to deepcopy all fields, especially the mutable ones
    }

    public Result executeReadOnly(Command command) {
        if (!command.readOnly()) {
            throw new IllegalArgumentException();
        }

        if (command instanceof AMOCommand) {
            return execute(command);
        }

        return application.execute(command);
    }

    public boolean alreadyExecuted(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;
        
        //TODO: check whether the amoCommand is already executed or not
        return false;
    }
}
