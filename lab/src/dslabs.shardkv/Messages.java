package dslabs.shardkv;

import dslabs.framework.Message;
import dslabs.paxos.AMOApplication;
import dslabs.paxos.AMOCommand;
import dslabs.paxos.AMOResult;
import java.util.HashMap;
import java.util.HashSet;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
final class ShardStoreRequest implements Message {
    // TODO: shard store request ...
    private int configNum;
    private AMOCommand command;

    public ShardStoreRequest(int configNum, AMOCommand amoCommand) {
        this.configNum = configNum;
        this.command = amoCommand;
    }
}

@Data
@EqualsAndHashCode
final class ShardTransferMsg implements Message {
    private final int transferMessageId;
    private final int configNum;
    private final int senderGroupId;
    private final int receiverGroupId;
    private final HashMap<Integer, AMOApplication> storeMap;
    private final HashSet<Integer> shardList;
}

@Data
@EqualsAndHashCode
final class ShardTransferAckMsg implements Message {
    private final int transferMessageId;
    private final int configNum;
    private final int senderGroupId;
    private final HashSet<Integer> shardList;
}


@Data
@EqualsAndHashCode
final class ShardStoreReply implements Message {
    // TODO: shard store reply ...
    private final int sequenceNum;
    private final AMOResult result;
}

@Data
@EqualsAndHashCode
final class ShardStoreAbortReply implements Message {
    // TODO: shard store reply ...
    private final int sequenceNum;
}

@Data
@EqualsAndHashCode
final class ShardError implements Message {
    // TODO: shard store reply ...
    private final int sequenceNum;
    private final int configNum;
}

// TODO: add more messages here ...
@Data
@EqualsAndHashCode
final class TxPhase1Msg implements Message {
    private final int coordinatorId;
    private final int coordinatorConfigId;
    private final AMOCommand command;
    private final HashSet<Integer> shardsToLock;
}

@Data
@EqualsAndHashCode
final class TxReadyMsg implements Message {
    private final int followerId;
    private final AMOCommand command;
}

@Data
@EqualsAndHashCode
final class TxAbortMsg implements Message {
    private final int followerId;
    private final int coordinatorId;
    private final int coordinatorConfigId;
    private final AMOCommand command;
}

@Data
@EqualsAndHashCode
final class TxAbortCommitMsg implements Message {
    private final int followerId;
    private final AMOCommand command;
}

@Data
@EqualsAndHashCode
final class TxPhase2Msg implements Message {
    private final int followerId;
    private final int coordinatorId;
    private final int coordinatorConfigId;
    private final AMOCommand command;
}

@Data
@EqualsAndHashCode
final class TxCommitMsg implements Message {
    private final int followerId;
    private final int configNum;
    private final HashSet<AMOResult> results;
    private final AMOCommand command;
}

@Data
@EqualsAndHashCode
final class TxROCommitMsg implements Message {
    private final int followerId;
    private final HashSet<AMOResult> results;
    private final AMOCommand command;
}
