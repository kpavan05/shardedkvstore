package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.Data;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application>
        implements Application {
    @Getter @NonNull private final T application;

    //TODO: declare fields for your implementation
    @Data
    private static final class Information implements Serializable {
        private final int sequenceNum;
        private final Result result;
    }
    @Getter private HashMap<Address, Information> recorder = new HashMap<>();

    @Override
    public AMOResult execute(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;

        //TODO: execute the command
        //Hints: remember to check whether the command is executed before and update records
        Information information;
        if (alreadyExecuted(amoCommand)) {
            information = recorder.get(amoCommand.address());
            return new AMOResult(information.result(), amoCommand.address(), information.sequenceNum());
        } else {
            // need to recording the value
            information = new Information(amoCommand.sequenceNum(), application.execute(amoCommand.command()));
            recorder.put(amoCommand.address(), information);
        }
        return new AMOResult(information.result(), amoCommand.address(), information.sequenceNum());
    }

    // copy constructor
    public AMOApplication(AMOApplication<Application> amoApplication)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        //TODO: a deepcopy constructor
        Application application = amoApplication.application();
        this.application = (T) application.getClass().getConstructor(application.getClass()).newInstance(application);
        //Hints: remember to deepcopy all fields, especially the mutable ones
        HashMap<Address, Information> sourceRecorder = amoApplication.recorder();
        for (Address address : sourceRecorder.keySet())
            this.recorder.put(address, sourceRecorder.get(address));
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
        Information information = null;
        if (recorder.containsKey(amoCommand.address()))
            information = recorder.get(amoCommand.address());
        return (information != null && information.sequenceNum() >= amoCommand.sequenceNum());
    }
}
