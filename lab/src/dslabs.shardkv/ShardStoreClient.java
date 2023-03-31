package dslabs.shardkv;

import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.kvstore.TransactionalKVStore.Transaction;
import dslabs.paxos.AMOCommand;
import dslabs.paxos.AMOResult;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.java.Log;
import org.apache.commons.lang3.SerializationUtils;

@Log
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
    //TODO: declare fields for your implementation ...

    ShardStoreRequest curRequest;
    ShardStoreRequest erroredRequest;
    Result curResult;
    int nextSequenceNum ;
    ShardConfig curConfig;
    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public ShardStoreClient(Address address, Address[] shardMasters,
                            int numShards) {
        super(address, shardMasters, numShards);
    }

    @Override
    public synchronized void init() {
        // TODO: initialize fields ...
        curRequest = null;
        curResult = null;
        nextSequenceNum = 1;
        curConfig = null;
        erroredRequest = null;
        broadcastToShardMasters(new PaxosRequest(address().toString(),0, new Query(-1)));
        set(new QueryTimer(), QueryTimer.QUERY_RETRY_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        // TODO: send command ...

        curResult = null;
        if (command instanceof Transaction) {
            erroredRequest = null;
            int coordinatorGroup = findCoordinator(command, curConfig);
           /* Transaction tr = (Transaction) command;
            Set<String> keys = tr.keySet();
            int maxGroupId = 0;
            int groupId = 0;
            for(String s: tr.keySet()) {
                int shardNum = keyToShard(s);
                groupId = findShardOwner(shardNum);
                if (groupId > 0 && groupId > maxGroupId) {
                    maxGroupId = groupId;
                }
            }*/
            AMOCommand amoCommand = new AMOCommand(command, address(), nextSequenceNum);
            int configNum = curConfig == null ? 0 : curConfig.configNum();
            curRequest = new ShardStoreRequest(configNum, amoCommand);
            if (curConfig != null) {
                broadcast(curRequest,
                        curConfig.groupInfo().get(coordinatorGroup).getLeft());
            }
            set(new ClientTimer(this.nextSequenceNum), ClientTimer.CLIENT_RETRY_MILLIS);
        } else {
            erroredRequest = null;
            SingleKeyCommand cmd = (SingleKeyCommand) command;
            int shardNum = keyToShard(cmd.key());

            AMOCommand amoCommand = new AMOCommand(command, address(), nextSequenceNum);
            int configNum = curConfig == null ? 0 : curConfig.configNum();
            curRequest = new ShardStoreRequest(configNum, amoCommand);
            if (curConfig != null) {
                int serverGroupId = findShardOwner(shardNum);
                broadcast(curRequest,
                        curConfig.groupInfo().get(serverGroupId).getLeft());
            }
            set(new ClientTimer(this.nextSequenceNum), ClientTimer.CLIENT_RETRY_MILLIS);
        }
    }


    @Override
    public synchronized boolean hasResult() {
        // TODO: check result available ...
        return curResult != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // TODO: get result ...
        while (!hasResult())
            wait();

        return curResult;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleShardStoreReply(ShardStoreReply m, Address sender) {
        // TODO: handle shard store reply ...
        if(curRequest != null && m.sequenceNum() == nextSequenceNum && m.result() != null) {
            curResult = m.result().result();
            nextSequenceNum++;
            notify();
        }
    }

    private synchronized void handleShardStoreAbortReply(ShardStoreAbortReply m, Address sender) {
        // TODO: handle shard store reply ...
        if(curRequest != null && m.sequenceNum() == nextSequenceNum) {
            curResult = null;
            nextSequenceNum++;
            erroredRequest = curRequest;
        }
    }

    private synchronized void handleShardError(ShardError m, Address sender) {
        if(curRequest != null && m.sequenceNum() == nextSequenceNum) {
                curResult = null;
                nextSequenceNum++;
                //broadcastToShardMasters(new PaxosRequest(address().toString(), curConfig.configNum(), new Query(-1)));
                erroredRequest = curRequest;
        }
    }
    // TODO: your message handlers ...

    private void handlePaxosReply(PaxosReply m, Address sender) {
        if (m.result() instanceof ShardConfig) {
            curConfig = (ShardConfig) m.result();
            if (erroredRequest != null) {
                //LOG.info("retrying " + erroredRequest);
                sendCommand(erroredRequest.command().command());
            }
        }
    }

    // TODO: add utils here ...
    private int findShardOwner(int shardNum) {
        for(int gid : curConfig.groupInfo().keySet()) {
            if (curConfig.groupInfo().get(gid).getRight().contains(shardNum)){
                return gid;
            }
        }
        return -1;
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // TODO: handle client request timeout ...
        if (t.sequenceNumber() == nextSequenceNum) {
            if(curConfig != null) {
                int serverGroupId = 0;

                if (curRequest.command().command() instanceof SingleKeyCommand) {
                    int shardNum = keyToShard(((SingleKeyCommand) curRequest.command().command()).key());
                    serverGroupId = findShardOwner(shardNum);
                }
                else if (curRequest.command().command() instanceof Transaction) {
                    serverGroupId = findCoordinator(curRequest.command().command(), curConfig);
                }
                curRequest.configNum(curConfig.configNum());
                broadcast(SerializationUtils.clone(curRequest), curConfig.groupInfo().get(serverGroupId).getLeft());
            }
            set(t, ClientTimer.CLIENT_RETRY_MILLIS);
        }
    }

    // TODO: add your time handlers ...
    private void onQueryTimer(QueryTimer t) {
        int seqNumber = curConfig == null ? 0: curConfig.configNum();

        broadcastToShardMasters(new PaxosRequest(address().toString(), seqNumber, new Query(-1)));
        set(t, QueryTimer.QUERY_RETRY_MILLIS + 20);
    }
}
