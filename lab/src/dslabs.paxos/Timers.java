package dslabs.paxos;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // TODO: add fields for client timer ...
    private final int sequenceNum;
}

// TODO: add more timers here ...