package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Result;
import lombok.Data;
import lombok.NonNull;

@Data
public final class AMOResult implements Result {
    //TODO: implement your wrapper for result
    //Hints: think carefully about what information is required for client to check duplication
    private final Result result;
    private final Address address;
    private final int sequenceNum;
}
