package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Command;
import lombok.Data;
import lombok.NonNull;

@Data
public final class AMOCommand implements Command {
    //TODO: implement your wrapper for command
    //Hints: think carefully about what information is required for server to check duplication
    private final Command command;
    private final Address address;
    private final int sequenceNum;
}
