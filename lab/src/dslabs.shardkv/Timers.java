package dslabs.shardkv;

import dslabs.framework.Timer;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // TODO: add fields for client request ...
    private final int sequenceNumber;
}

// TODO: add more timers here ...
@Data
@EqualsAndHashCode
final class QueryTimer implements Timer {
    static final int QUERY_RETRY_MILLIS = 50;
}

@Data
@EqualsAndHashCode
final class TransferTimer implements  Timer {
    static final int TRANSFER_RETRY_MILLIS = 55;
    private final int messageId;
    private final int configNum;
}

@Data
@EqualsAndHashCode
final class TxPhase1Timer implements  Timer {
    static final int TXPHASE1_RETRY_MILLIS = 45;
    private final TxPhase1Msg m;
    private final int groupId;
}

@Data
@EqualsAndHashCode
final class TxPhase2Timer implements  Timer {
    static final int TXPHASE2_RETRY_MILLIS = 45;
    private final TxPhase2Msg m;
    private final int groupId;
}

@Data
@EqualsAndHashCode
final class TxAbortTimer implements  Timer {
    static final int TXABORT_RETRY_MILLIS = 45;
    private final TxAbortMsg m;
    private final int groupId;
}
