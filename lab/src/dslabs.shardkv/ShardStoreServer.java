package dslabs.shardkv;


import dslabs.framework.Address;
import dslabs.framework.Command;

import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.kvstore.TransactionalKVStore;
import dslabs.kvstore.TransactionalKVStore.MultiGet;
import dslabs.kvstore.TransactionalKVStore.MultiGetResult;
import dslabs.kvstore.TransactionalKVStore.MultiPut;
import dslabs.kvstore.TransactionalKVStore.MultiPutOk;
import dslabs.kvstore.TransactionalKVStore.Swap;
import dslabs.kvstore.TransactionalKVStore.Transaction;
import dslabs.paxos.AMOApplication;
import dslabs.paxos.AMOCommand;
import dslabs.paxos.AMOResult;
import dslabs.paxos.PaxosDecision;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.paxos.PaxosServer;

import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.java.Log;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.MutablePair;


@Log
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
    private final Address[] group;
    private final int groupId;

    // TODO: declare fields for your implementation ...
    private static final String PAXOS_ADDRESS_ID = "paxos";
    private Address paxosAddress;

    private HashMap<String, MutablePair<Integer, AMOResult>> executedRequests = new HashMap<>();
    private HashMap<Integer, AMOApplication> storeMap = new HashMap<>();

    private HashMap<Integer, Integer> curShardMap = new HashMap<>();
    private HashMap<Integer, Integer> pendingShardMap = new HashMap<>();
    private HashMap<Integer, ShardTransferMsg> transferMessageMap = new HashMap<>();
    private Queue<PaxosDecision> decisionSet = new LinkedList<>();

    private HashMap<String, MutablePair<Integer, Boolean>> paxosRequestMap = new HashMap<>();
    private HashMap<Integer, Boolean> transferMap = new HashMap<>();

    private HashMap<Integer, MutablePair<HashSet<Integer>, Integer>> txProgressMap = new HashMap<>();
    private HashSet<Integer> lockSet = new HashSet<>();
    private AMOResult curTxResult = null;
    private Boolean txInProgress = false;
    private HashMap<String, MutablePair<Integer, Integer>> danglingAborts = new HashMap<>();
    private LinkedList<Integer> followerQ = new LinkedList<>();

    private Integer transferMessageId;
    private ShardConfig curConfig;
    private ShardConfig pendingConfig;
    /* -------------------------------------------------------------------------
    Construction and initialization
   -----------------------------------------------------------------------*/
    ShardStoreServer(Address address, Address[] shardMasters, int numShards,
                     Address[] group, int groupId) {
        super(address, shardMasters, numShards);
        this.group = group;
        this.groupId = groupId;

        // TODO: ...

        //this.app = new AMOApplication<Application>(new KVStore());
        transferMessageId = new Integer(0);
        curConfig = null;
        pendingConfig = null;
    }

    @Override
    public void init() {
        // TODO: initialize fields ...
        pendingShardMap.clear();
        curShardMap.clear();
        transferMessageMap.clear();
        decisionSet.clear();
        danglingAborts.clear();

        curConfig = null;
        pendingConfig = null;
        curTxResult = null;
        // Setup Paxos
        paxosAddress = Address.subAddress(address(), PAXOS_ADDRESS_ID);

        Address[] paxosAddresses = new Address[group.length];
        for (int i = 0; i < paxosAddresses.length; i++) {
            paxosAddresses[i] = Address.subAddress(group[i], PAXOS_ADDRESS_ID);
        }

        PaxosServer paxosServer =
                new PaxosServer(paxosAddress, paxosAddresses, address());
        addSubNode(paxosServer);
        paxosServer.init();

        broadcastToShardMasters(new PaxosRequest(address().toString(), 0, new Query(0)));
        set(new QueryTimer(), QueryTimer.QUERY_RETRY_MILLIS);
    }



    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
        // TODO: handle paxos request ...
        // perform validation -> possible pre-pocessing
        String sid = m.command().address().toString() + "-" + m.configNum();
        if (curConfig == null || m.configNum() != curConfig.configNum() ||
                curConfig.configNum() != pendingConfig.configNum()) {
            ////printLogInfo(m + " request dropped");
            return;
        }
        if (executedRequests.containsKey(sid /*sender.toString()*/) ) {
            int sequenceNum = executedRequests.get(sid).getLeft();
            if (m.command().sequenceNum() < sequenceNum) {
                return;
            }

            if (m.command().sequenceNum() == sequenceNum) {
                AMOResult res = executedRequests.get(sid).getRight();
                if (res != null) {
                    send(new ShardStoreReply(m.command().sequenceNum(), res),sender);
                    return;
                } else {
                    return;
                }
            }
        }
        if (txInProgress) {
            //printTxLogInfo(curRequest + " still running");
            return;
        }

        //printTxLogInfo("accepted " + m.command().toString());
        //curRequest = m.command();
        if (!executedRequests.containsKey(sid)) {
            MutablePair<Integer, AMOResult> p = new MutablePair<>(m.command().sequenceNum(), null);
            executedRequests.put(sid, p);
        } else {
            MutablePair<Integer, AMOResult> p = executedRequests.get(sid);
            p.setLeft(m.command().sequenceNum());
            p.setRight(null);
        }

        if (m.command().command() instanceof Transaction) {

            int gid = findCoordinator(m.command().command(), curConfig);
            if (groupId == gid) {
                curTxResult = null;
                txInProgress = true;
                startTransaction(m.command());
            }

        } else {
            SingleKeyCommand cmd = (SingleKeyCommand) m.command().command();
            int shardNum = keyToShard(cmd.key());
            if (!curShardMap.containsKey(shardNum) ||
                    curShardMap.get(shardNum) != groupId) {
                ////printLogInfo(" request for incorrect shard number");
                send(new ShardStoreReply(m.command().sequenceNum(), null),
                        sender);
                return;
            }
            ClientCmd clCommand = new ClientCmd(m.configNum(), m.command());
            //process(m.command(), false);
            process(clCommand, false);
        }
    }

    private void startTransaction(AMOCommand command)  {
        Transaction tr = (Transaction)command.command();
        //printTxLogInfo("started txn " + command +  " at group " + groupId);
        Set<String> keys = tr.keySet();
        txProgressMap.clear();
        followerQ.clear();
        for(String s: tr.keySet()) {
            int shardNum = keyToShard(s);
            int gid = findShardOwner(shardNum, curConfig);
            if (gid != groupId && !followerQ.contains(gid)) {
                followerQ.add(gid);
            }
            if (!txProgressMap.containsKey(gid)) {
                MutablePair<HashSet<Integer>, Integer> p = new MutablePair<>(new HashSet<>(), 0);
                p.getLeft().add(shardNum);
                txProgressMap.put(gid, p);
            } else {
                txProgressMap.get(gid).getLeft().add(shardNum);
            }
        }
        followerQ.add(groupId);
        AMOCommand curCommand = SerializationUtils.clone(command);

        Integer gid = followerQ.pollFirst();
        TxPhase1Msg p = new TxPhase1Msg(groupId, curConfig.configNum(),curCommand,
                SerializationUtils.clone(txProgressMap.get(gid).getLeft()));
        if (gid == groupId) {
            txProgressMap.get(groupId).setRight(1);
            handleMessage(p);
        } else {
            Set<Address> to = curConfig.groupInfo().get(gid).getLeft();
            broadcast(p, to);
            txProgressMap.get(gid).setRight(1);
        }

    }

    private void startTxPhase2(AMOCommand command) {
        boolean hasFollowers = false;
        for (int gid : txProgressMap.keySet()) {
            if (groupId == gid || txProgressMap.get(gid).getRight() == 11) {
                continue;
            }
            hasFollowers = true;
            TxPhase2Msg p2 = new TxPhase2Msg(gid, groupId, curConfig.configNum(),command);
            Set<Address> to = curConfig.groupInfo().get(gid).getLeft();
            broadcast(p2, to);
            txProgressMap.get(gid).setRight(11);
        }
        if (!hasFollowers) {
            TxPhase2Msg p2 = new TxPhase2Msg(groupId, groupId, curConfig.configNum(),command);
            handleMessage(p2);
        }
    }

    private void process(Command command, boolean replicated) {
        if (command instanceof ShardMoveCmd) {
            processShardMove((ShardMoveCmd) command, replicated);
        } else if (command instanceof ShardMoveAckCmd) {
            processShardMoveAck((ShardMoveAckCmd) command, replicated);
        } else if (command instanceof NewConfigCmd) {
            processNewConfig((NewConfigCmd) command, replicated);
        } else if (command instanceof AMOCommand) {
            processAMOCommand((AMOCommand) command, replicated);
        } else if (command instanceof TxPhase1Cmd) {
            processTxPhase1Cmd((TxPhase1Cmd)command, replicated);
        } else if (command instanceof TxPhase2Cmd) {
            processTxPhase2Cmd((TxPhase2Cmd)command, replicated);
        } else if (command instanceof TxCommitCmd) {
            processTxCommitCmd((TxCommitCmd)command, replicated);
        } else if (command instanceof TxAbortCmd) {
            processTxAbortCmd((TxAbortCmd)command, replicated);
        } else if (command instanceof TxAbortCommitCmd) {
            processTxAbortCommitCmd((TxAbortCommitCmd)command, replicated);
        }else if (command instanceof ClientCmd) {
            processClientCmd((ClientCmd) command, replicated);
        }

    }

    private void setCurrentConfig(ShardConfig config) {
        curConfig = SerializationUtils.clone(config);
        curShardMap.clear();
        curShardMap.putAll(pendingShardMap);

        while(decisionSet.size() != 0) {
            PaxosDecision d = decisionSet.poll();
            process(d.command(), true);
        }
    }

    // TODO: your message handlers ...
    private void handleShardTransferMsg(ShardTransferMsg m , Address sender) {
        ////printLogInfo("group " + groupId + " got move message "+ m.configNum() + " pending config " + pendingConfig.configNum());
        if (pendingConfig == null || m.configNum() > pendingConfig.configNum()) {
            return;
        }
        if (m.configNum() == curConfig.configNum() || m.configNum() < pendingConfig.configNum()) {
            send(new ShardTransferAckMsg(m.transferMessageId(), m.configNum(),groupId,
                    m.shardList()), sender );
            return;
        }

        ////printLogInfo("group " + groupId + " received transfer msg for " + m.shardList() + " from " + m.senderGroupId());
        ShardMoveCmd cmd = new ShardMoveCmd(pendingConfig.configNum(),
                m.configNum(), m.senderGroupId(), m.storeMap(), m.shardList(),m.transferMessageId());
        processShardMove( cmd,false);
    }

    private void handleShardTransferAckMsg(ShardTransferAckMsg m, Address sender) {
        ////printLogInfo("group " + groupId + " got ack message " + m.configNum() + " pending config " + curConfig.configNum());
        if (pendingConfig == null || m.configNum() != pendingConfig.configNum()) {
            return;
        }

        ////printLogInfo("group " + groupId + " received transfer ack msg for " + m.shardList() + " from " + m.senderGroupId());
        if (!transferMessageMap.containsKey(m.transferMessageId())) {
            return;
        }
        transferMessageMap.put(m.transferMessageId(), null);
        transferMessageMap.remove(m.transferMessageId());

        ShardMoveAckCmd cmd = new ShardMoveAckCmd(pendingConfig.configNum(),
                m.configNum(), m.senderGroupId(), m.shardList());
        process(cmd, false);
    }

    private void handleTxPhase1Msg(TxPhase1Msg m, Address sender) {
        String rid = "co:" + m.coordinatorId() + "-cid:" + m.command().address().toString() +
                "-" + m.coordinatorConfigId() + "-P1";
        if (isRequestProcessed(rid, m.command().sequenceNum())) {
            return;
        }
        if (danglingAborts.containsKey(m.command().address().toString())) {
            MutablePair<Integer, Integer> p = danglingAborts.get(m.command().address().toString());
            if (p.getLeft() == m.coordinatorConfigId() && p.getRight() == m.command().sequenceNum()) {
                return;
            }
        }
        if (m.coordinatorConfigId() != curConfig.configNum() || curConfig.configNum() != pendingConfig.configNum() ||
                (m.coordinatorId() != groupId && txInProgress)) {
            if (!danglingAborts.containsKey(m.command().address().toString())) {
                danglingAborts.put(m.command().address().toString(),
                        new MutablePair<Integer, Integer>(m.coordinatorConfigId(), m.command().sequenceNum()));
            } else {
                MutablePair<Integer, Integer> p = danglingAborts.get(m.command().address().toString());
                p.setLeft(m.coordinatorConfigId());
                p.setRight(m.command().sequenceNum());
            }
            send(new TxAbortMsg(groupId, m.coordinatorId(), m.coordinatorConfigId(),m.command()), sender);
            return;
        }
        txInProgress = true;
        TxPhase1Cmd cmd = new TxPhase1Cmd(m.coordinatorId(), m.coordinatorConfigId(), m.command(), m.shardsToLock());
        process(cmd, false);

        if (m.coordinatorId() == groupId) {
            if (!m.command().readOnly() && isPhase1Done()) {
                startTxPhase2(m.command());
            }
        }
    }

    private void handleTxReadyMsg(TxReadyMsg m, Address sender) {
        if (!txInProgress) {
            return;
        }
        if (txProgressMap.containsKey(m.followerId())) {
            if (txProgressMap.get(m.followerId()).getRight() == 2) {
                return;
            }
            txProgressMap.get(m.followerId()).setRight(2);
        }
        //txProgressMap.get(m.followerId()).setRight(2);

        if (followerQ.size() != 0) {
            Integer gid = followerQ.pollFirst();
            TxPhase1Msg p =
                    new TxPhase1Msg(groupId, curConfig.configNum(), m.command(),
                            SerializationUtils.clone(txProgressMap.get(gid).getLeft()));
            if (gid == groupId) {
                txProgressMap.get(groupId).setRight(1);
                handleMessage(p);
            } else {
                Set<Address> to = curConfig.groupInfo().get(gid).getLeft();
                broadcast(p, to);
                txProgressMap.get(gid).setRight(1);
            }
        }
    }

    private void handleTxAbortMsg(TxAbortMsg m, Address sender) {
        if (txProgressMap.containsKey(m.followerId())) {
            if (txProgressMap.get(m.followerId()).getRight() == 0) {
                return;
            }
            txProgressMap.get(m.followerId()).setRight(0);
        }
        if (m.coordinatorId() == groupId) {
            /*if (txProgressMap.containsKey(m.followerId()))
                txProgressMap.get(m.followerId()).setRight(0);*/
            boolean hasP1Followers = false;
            for (Integer gid : txProgressMap.keySet()) {
                if (gid == groupId) continue;
                if (txProgressMap.get(gid).getRight() == 2) {
                    hasP1Followers = true;
                    Set<Address> to = curConfig.groupInfo().get(gid).getLeft();
                    broadcast(new TxAbortMsg(gid, groupId, curConfig.configNum(), m.command()), to);
                    continue;
                }
                if (followerQ.contains(gid)) {
                    txProgressMap.get(gid).setRight(0);
                }
            }
            followerQ.clear();
            if (!hasP1Followers) {
                //printTxLogInfo("no followers progressed phase 1 for " + m.command());
                handleMessage(new TxAbortCommitMsg(groupId, m.command()));
            }
        } else {
            TxAbortCmd cmd = new TxAbortCmd(m.coordinatorId(), m.coordinatorConfigId(), m.command());
            process(cmd, false);
        }
    }

    private void handleTxAbortCommitMsg(TxAbortCommitMsg m, Address sender) {
        String rid = m.command().address().toString() + "-" + curConfig.configNum() + "-abort";
        if (isRequestProcessed(rid, m.command().sequenceNum())) {
            return;
        }
        if (txProgressMap.containsKey(m.followerId())) {
            txProgressMap.get(m.followerId()).setRight(0);
        }
        //txProgressMap.get(m.followerId()).setRight(0);

        if (hasFollowersAborted()) {
            TxAbortCommitCmd cmd = new TxAbortCommitCmd(groupId, curConfig.configNum(), m.command());
            process(cmd, false);
        }
    }

    private void handleTxPhase2Msg(TxPhase2Msg m, Address sender){
        String rid = "co:" + m.coordinatorId() + "-cid:" + m.command().address().toString() +
                "-" + m.coordinatorConfigId() + "-P2";
        if (isRequestProcessed(rid, m.command().sequenceNum())) {
            return;
        }
        if (m.coordinatorConfigId() != curConfig.configNum() || curConfig.configNum() != pendingConfig.configNum()) {
            return;
        }
        String cid = m.command().address().toString();
        if (danglingAborts.containsKey(cid)) {
            if (danglingAborts.get(cid).getRight() <= m.command().sequenceNum()) {
                danglingAborts.remove(cid);
            }
        }
        TxPhase2Cmd command = new TxPhase2Cmd(m.coordinatorId(), m.coordinatorConfigId(), m.command());
        process(command, false);

        if (isPhase2Done() && m.coordinatorId() == groupId) {
            TxCommitCmd commitCmd = new TxCommitCmd(groupId, curConfig.configNum(),m.command());
            process(commitCmd, false);
        }
    }

    private void handleTxCommitMsg(TxCommitMsg m, Address sender) {
        String rid = m.command().address().toString() + "-" + m.configNum();
        if (isRequestProcessed(rid, m.command().sequenceNum())) {
            return;
        }
        //String requestId = m.command().address().toString();
        /*if (executedRequests.containsKey(requestId)) {
            Integer seqNum = executedRequests.get(requestId).getLeft();
            if(m.command().sequenceNum() == seqNum) {
                if (executedRequests.get(requestId).getRight() != null)
                    return;
            }
            if (m.command().sequenceNum() < seqNum) {
                return;
            }
        }*/

        if(txProgressMap.containsKey(m.followerId())) {
            if (txProgressMap.get(m.followerId()).getRight() == 12) {
                return;
            }
            txProgressMap.get(m.followerId()).setRight(12);
        }
        //txProgressMap.get(m.followerId()).setRight(12);

        String id = m.command().address().toString()+"-" + curConfig.configNum();
        for(AMOResult r : m.results()) {
            if (r.result() instanceof MultiPutOk) {
                curTxResult = r;
            } else if (r.result() instanceof MultiGetResult) {
                if (curTxResult == null) {
                    curTxResult = r ; //SerializationUtils.clone(r);
                } else {
                    MultiGetResult curResult = (MultiGetResult) r.result();
                    ((MultiGetResult)curTxResult.result()).values().putAll(curResult.values());
                }
            }
        }
        if (hasFollowersCommitted() &&  groupId != m.followerId() ) {
            TxPhase2Msg p2 = new TxPhase2Msg(groupId, groupId, curConfig.configNum(),m.command());
            handleMessage(p2);
        }
    }

    private void handleTxROCommitMsg(TxROCommitMsg m, Address sender) {
        if(!txProgressMap.containsKey(m.followerId())) {
            //printTxLogInfo("something is wrong");
        }
        txProgressMap.get(m.followerId()).setRight(12);

        String id = m.command().address().toString()+"-" + curConfig.configNum();
        for(AMOResult r : m.results()) {
            if (r.result() instanceof MultiGetResult) {
                AMOResult res = executedRequests.get(id).getRight();
                if (curTxResult == null) {
                    curTxResult = SerializationUtils.clone(r);
                } else {
                    MultiGetResult curResult = (MultiGetResult) r.result();
                    ((MultiGetResult)curTxResult.result()).values().putAll(curResult.values());
                }
            }
        }
        if (followerQ.size() != 0) {
            Integer gid = followerQ.poll();
            TxPhase1Msg p =
                    new TxPhase1Msg(groupId, curConfig.configNum(), m.command(),
                            SerializationUtils.clone(txProgressMap.get(gid).getLeft()));
            if (gid == groupId) {
                txProgressMap.get(groupId).setRight(1);
                handleMessage(p);
            } else {
                Set<Address> to = curConfig.groupInfo().get(gid).getLeft();
                broadcast(p, to);
                txProgressMap.get(gid).setRight(1);
            }
        }
    }

    private void postDecisionProcess(String id, AMOCommand command, int coordinatorId) {
        if (command.command() instanceof Transaction) {
            Transaction tx = (Transaction) command.command();
            if (tx instanceof MultiPut) {
                HashMap<Integer, HashMap<String, String>> shardKVPair = new HashMap<>();
                for (String s : tx.keySet()) {
                    int shardNum = keyToShard(s);
                    int gid = findShardOwner(shardNum, curConfig);
                    if (gid == groupId) {
                        if (!shardKVPair.containsKey(shardNum)) {
                            HashMap<String, String> kvPair = new HashMap<String,String>();
                            shardKVPair.put(shardNum, kvPair);
                        }
                        shardKVPair.get(shardNum).put(s, ((MultiPut) tx).values().get(s));
                    }
                }
                HashSet<AMOResult> resultSet = new HashSet<>();
                for(Integer shardId:shardKVPair.keySet()) {
                    MultiPut subTxn = new MultiPut(shardKVPair.get(shardId));
                    AMOCommand cmd = new AMOCommand(subTxn, command.address(), command.sequenceNum());
                    AMOResult res = executeAMOCommand(cmd, shardId); //storeMap.get(shardId).execute(subTxn);
                    resultSet.add(res);
                    lockSet.remove(shardId);
                }
                shardKVPair.clear();
                if (coordinatorId == groupId) {
                    handleMessage(new TxCommitMsg(groupId, curConfig.configNum(), resultSet, command));
                } else {
                    Set<Address> to = curConfig.groupInfo().get(coordinatorId).getLeft();
                    broadcast(new TxCommitMsg(groupId, curConfig.configNum(), resultSet, command), to);
                    txInProgress = false;
                }

            } else if (tx instanceof MultiGet) {
                HashMap<Integer, HashSet<String>> shardKVPair = new HashMap<>();
                for (String s : tx.keySet()) {
                    int shardNum = keyToShard(s);
                    int gid = findShardOwner(shardNum, curConfig);
                    if (gid == groupId) {
                        if (!shardKVPair.containsKey(shardNum)) {
                            HashSet<String> keys = new HashSet<String>();
                            shardKVPair.put(shardNum,keys);

                        }
                        shardKVPair.get(shardNum).add(s);
                    }
                }
                HashSet<AMOResult> resultSet = new HashSet<>();
                for(Integer shardId:shardKVPair.keySet()) {
                    MultiGet subTxn = new MultiGet(shardKVPair.get(shardId));
                    AMOCommand cmd = new AMOCommand(subTxn, command.address(), command.sequenceNum());
                    AMOResult res = executeAMOCommand(cmd, shardId);
                    resultSet.add(res);
                    lockSet.remove(shardId);
                }
                shardKVPair.clear();
                if (coordinatorId == groupId) {
                    handleMessage(new TxCommitMsg(groupId, curConfig.configNum(), resultSet, command));
                } else {
                    Set<Address> to = curConfig.groupInfo().get(coordinatorId).getLeft();
                    broadcast(new TxCommitMsg(groupId, curConfig.configNum(), SerializationUtils.clone(resultSet)/*resultSet*/, command), to);
                    txInProgress = false;
                }

            } else if (tx instanceof Swap) {

            }

        } else {
            SingleKeyCommand cmd = (SingleKeyCommand) command.command();
            int shardNum = keyToShard(cmd.key());
            if (curShardMap.get(shardNum) != groupId) {
                ////printLogInfo("group " + groupId + " not responsible for " + shardNum + " incorrect request");
                //LOG.severe("incorrect request config:" + curConfig.configNum());
                //send(new ShardError(command.sequenceNum(), curConfig.configNum()), command.address());
                return;
            }
            AMOResult  res = null;
            if (storeMap.containsKey(shardNum)) {
                ////printLogInfo("group " + groupId + " added " + command + " to shard " + shardNum);
                AMOApplication app = storeMap.get(shardNum);
                res = app.execute(command);
            } else {

                AMOApplication app = new AMOApplication(new TransactionalKVStore());
                res = app.execute(command);
                storeMap.put(shardNum, app);
            }
            if (!executedRequests.containsKey(command.address().toString())) {
                MutablePair<Integer, AMOResult> p = new MutablePair<>(command.sequenceNum(), res);
                executedRequests.put(command.address().toString(), p);
            } else {
                MutablePair<Integer, AMOResult> p = executedRequests.get(command.address().toString());
                p.setLeft(command.sequenceNum());
                p.setRight(res);
            }
            //AMOResult res = executeAMOCommand(command, shardNum);
            //executedRequests.get(id).setRight(res);
            send(new ShardStoreReply(command.sequenceNum(), res), command.address());
        }
    }
    // TODO: your command process ...


    private void processClientCmd(ClientCmd command, boolean replicated) {
        // generate local proposing `PaxosRequest`
        String id = command.amoCommand.address().toString() + "-" + command.configNum();
        if (!replicated) {
            this.handleMessage(new PaxosRequest(id,command.amoCommand.sequenceNum(),
                    command), paxosAddress);
            return;
        }

        AMOCommand amoCommand = command.amoCommand;
        SingleKeyCommand cmd = (SingleKeyCommand) amoCommand.command();
        int shardNum = keyToShard(cmd.key());
        if (curShardMap.get(shardNum) != groupId) {
            ////printLogInfo("group " + groupId + " not responsible for " + shardNum + " incorrect request");
            //LOG.severe("incorrect request config:" + curConfig.configNum());
            //send(new ShardError(command.sequenceNum(), curConfig.configNum()), command.address());
            return;
        }
        AMOResult  res = null;
        if (storeMap.containsKey(shardNum)) {
            ////printLogInfo("group " + groupId + " added " + command + " to shard " + shardNum);
            AMOApplication app = storeMap.get(shardNum);
            res = app.execute(amoCommand);
        } else {

            AMOApplication app = new AMOApplication(new TransactionalKVStore());
            res = app.execute(amoCommand);
            storeMap.put(shardNum, app);
        }
        if (!executedRequests.containsKey(id /*command.address().toString()*/)) {
            MutablePair<Integer, AMOResult> p = new MutablePair<>(amoCommand.sequenceNum(), res);
            executedRequests.put(id /*command.address().toString()*/, p);
        } else {
            MutablePair<Integer, AMOResult> p = executedRequests.get(id /*command.address().toString()*/);
            p.setLeft(amoCommand.sequenceNum());
            p.setRight(res);
        }
        send(new ShardStoreReply(amoCommand.sequenceNum(), res), amoCommand.address());
    }

    private AMOResult executeAMOCommand(AMOCommand command, int shardNum) {
        AMOResult  res = null;
        if (storeMap.containsKey(shardNum)) {
            ////printLogInfo("group " + groupId + " added " + command + " to shard " + shardNum);
            AMOApplication app = storeMap.get(shardNum);
            res = app.execute(command);
        } else {

            AMOApplication app = new AMOApplication(new TransactionalKVStore());
            res = app.execute(command);
            storeMap.put(shardNum, app);
        }
        /*
        if (!executedRequests.containsKey(command.address().toString())) {
            MutablePair<Integer, AMOResult> p = new MutablePair<>(command.sequenceNum(), res);
            executedRequests.put(command.address().toString(), p);
        } else {
            MutablePair<Integer, AMOResult> p = executedRequests.get(command.address().toString());
            p.setLeft(command.sequenceNum());
            p.setRight(res);
        }*/

        return res;
    }
    private void sendPaxosRequest(String id, Command command) {
        try {
            if (command instanceof ShardMoveCmd) {
                this.handleMessage(
                        new PaxosRequest(id,((ShardMoveCmd) command).sequenceNum(), command),
                        paxosAddress);
            } else if (command instanceof ShardMoveAckCmd) {
                this.handleMessage(
                        new PaxosRequest(id,((ShardMoveAckCmd) command).sequenceNum(), command),
                        paxosAddress);
            } else if (command instanceof NewConfigCmd) {
                this.handleMessage(new PaxosRequest(id, ((NewConfigCmd) command).sequenceNum(), command),
                        paxosAddress);
            } else if (command instanceof TxPhase1Cmd) {
                this.handleMessage(new PaxosRequest(id, ((TxPhase1Cmd) command).amoCommand.sequenceNum(), command),
                        paxosAddress);
            } else if (command instanceof TxReadyCmd) {
                this.handleMessage(new PaxosRequest(id, ((TxPhase1Cmd) command).amoCommand.sequenceNum(), command),
                        paxosAddress);
            } else if (command instanceof TxPhase2Cmd) {
                this.handleMessage(new PaxosRequest(id, ((TxPhase2Cmd) command).amoCommand.sequenceNum(), command),
                        paxosAddress);
            } else if (command instanceof TxCommitCmd) {
                this.handleMessage(new PaxosRequest(id, ((TxCommitCmd) command).amoCommand.sequenceNum(), command),
                        paxosAddress);
            } else if (command instanceof TxAbortCmd) {
                this.handleMessage(new PaxosRequest(id, ((TxAbortCmd) command).amoCommand.sequenceNum(), command),
                        paxosAddress);
            } else if (command instanceof TxAbortCommitCmd) {
                this.handleMessage(new PaxosRequest(id, ((TxAbortCommitCmd) command).amoCommand.sequenceNum(), command),
                        paxosAddress);
            }

        } catch (NullPointerException x) {
            LOG.severe(x.getStackTrace().toString());
        }
    }

    private void processNewConfig(NewConfigCmd command, boolean replicated) {
        String rid = groupId + "-ShardConfig";

        // generate local proposing `PaxosRequest`
        if (!replicated) {
            if (isDuplicateRequest(rid, command.sequenceNum)) {
                return;
            }
            addToPaxosRequestMap(rid, command.sequenceNum);
            sendPaxosRequest(rid, command);
            return;
        }
        if (pendingConfig != null && pendingConfig.configNum() >= command.config.configNum()) {
            return;
        }
        markRequestDone(rid, command.sequenceNum);
        pendingConfig = command.config;
        makeShardMap(pendingConfig);

        /*if (curConfig != null) {
            ////printLogInfo("group " + groupId + " server:" + address().toString() + " moving from config " + curConfig.configNum() + " to " + pendingConfig.configNum());
        } else {
            ////printLogInfo("group " + groupId + " server:" + address().toString() + " moving from no config  "  + " to " + pendingConfig.configNum());
        }*/

        if (curConfig == null || (!curShardMap.containsValue(groupId) && !pendingShardMap.containsValue(groupId))) {
            setCurrentConfig(pendingConfig);
            return;
        }

        Map<Integer, HashSet<Integer>> outgoingGroupShardMap = getOutgoingGroupShardMap();
        getIncomingGroupShardMap();

        for (int gid: outgoingGroupShardMap.keySet()) {
            transferMessageId++;
            HashMap<Integer, AMOApplication> copy = new HashMap<>();
            for(int s : outgoingGroupShardMap.get(gid)) {
                if (storeMap.get(s) == null) continue;
                copy.put(s, SerializationUtils.clone(storeMap.get(s)));
                //copy.put(s, new AMOApplication(storeMap.get(s).application()));
            }
            ShardTransferMsg msg = new ShardTransferMsg(transferMessageId, pendingConfig.configNum(), groupId, gid,
                    copy, SerializationUtils.clone(outgoingGroupShardMap.get(gid)));

            transferMessageMap.put(transferMessageId, msg);
            broadcast(msg,pendingConfig.groupInfo().get(gid).getLeft());
            set(new TransferTimer(transferMessageId, pendingConfig.configNum()), TransferTimer.TRANSFER_RETRY_MILLIS);
        }
        outgoingGroupShardMap.clear();

        if (hasExecutedTransfers()) {
            setCurrentConfig(pendingConfig);
        }
    }

    private void processShardMove(ShardMoveCmd command, boolean replicated) {
        String rid = command.senderGroupId + "-ShardMove";

        // generate local proposing `PaxosRequest`
        if (!replicated) {
            if (isDuplicateRequest(rid, command.sequenceNum)) {
                return;
            }
            addToPaxosRequestMap(rid, command.sequenceNum);
            sendPaxosRequest(rid, command);
            return;
        }
        markRequestDone(rid, command.sequenceNum);
        //add to the store map
        for(int shardId: command.shardSet) {
            transferMap.put(shardId, true);
            if (!command.storeMap.containsKey(shardId) )  {
                ////printLogInfo("group " + groupId + " got empty store with shard id: " + shardId + " in pending" + " config " + pendingConfig.configNum());
                continue;
            }
            storeMap.put(shardId, SerializationUtils.clone(command.storeMap.get(shardId)));
            //AMOApplication shardApp = new AMOApplication(command.storeMap.get(shardId).application());
            //storeMap.put(shardId, shardApp);
        }
        ////printLogInfo(" added shards "+ command.shardSet + " from " + command.senderGroupId + " to " + groupId);
        Set<Address> to = curConfig.groupInfo().get(command.senderGroupId).getLeft();
        broadcast(new ShardTransferAckMsg(command.msgId(),pendingConfig.configNum(),groupId,command.shardSet()), to );

        if (hasExecutedTransfers()) {
            ////printLogInfo("group " + groupId + " server:" + address().toString() + " moved to config " + pendingConfig.configNum());
            setCurrentConfig(pendingConfig);
        }
    }

    private void processShardMoveAck(ShardMoveAckCmd command, boolean replicated) {
        String rid = command.senderGroupId + "-ShardMoveAck";

        // generate local proposing `PaxosRequest`
        if (!replicated) {
            if (isDuplicateRequest(rid, command.sequenceNum)) {
                return;
            }
            addToPaxosRequestMap(rid, command.sequenceNum);
            sendPaxosRequest(rid, command);
            return;
        }

        markRequestDone(rid, command.sequenceNum);
        for(int shardId: command.shardSet) {
            transferMap.put(shardId, true);
            storeMap.put(shardId, null);
            storeMap.remove(shardId);

        }
        ////printLogInfo(" removed shards "+ command.shardSet + " from " + groupId);

        // ack received from all groups, safe to change the curConfig
        if (hasExecutedTransfers()) {
            ////printLogInfo("group " + groupId + " moved to config " + pendingConfig.configNum());
            setCurrentConfig(pendingConfig);
        }
    }

    private void processTxPhase1Cmd(TxPhase1Cmd command, boolean replicated) {
        String rid = "co:" + command.coordinatorId + "-cid:" + command.amoCommand.address().toString() +
                "-" +curConfig.configNum() + "-P1";

        // generate local proposing `PaxosRequest`
        if (!replicated) {
            if (isDuplicateRequest(rid, command.amoCommand.sequenceNum())) {
                return;
            }
            addToPaxosRequestMap(rid, command.amoCommand.sequenceNum());
            sendPaxosRequest(rid, command);
            return;
        }
        if (isRequestProcessed(rid, command.amoCommand.sequenceNum())) {
            return;
        }
        markRequestDone(rid, command.amoCommand.sequenceNum());

        for(Integer s: command.shardsToLock) {
            lockSet.add(s);
        }
        if (command.coordinatorId != groupId) {
            Set<Address> to = curConfig.groupInfo().get(command.coordinatorId).getLeft();
            broadcast(new TxReadyMsg(groupId, command.amoCommand), to);
        } else {
            handleMessage(new TxReadyMsg(groupId, command.amoCommand));
        }
    }

    private void processTxPhase2Cmd(TxPhase2Cmd command, boolean replicated) {
        String rid = "co:" + command.coordinatorId + "-cid:" + command.amoCommand.address().toString() +
                "-" + curConfig.configNum() + "-P2";
        // generate local proposing `PaxosRequest`
        if (!replicated) {
            if (isDuplicateRequest(rid, command.amoCommand.sequenceNum())) {
                return;
            }
            addToPaxosRequestMap(rid, command.amoCommand.sequenceNum());
            sendPaxosRequest(rid, command);
            return;
        }
        if (isRequestProcessed(rid, command.amoCommand.sequenceNum())) {
            return;
        }
        markRequestDone(rid, command.amoCommand.sequenceNum());

        AMOCommand cmd = command.amoCommand;
        postDecisionProcess(rid, cmd, command.coordinatorId);
    }

    private void processTxAbortCmd(TxAbortCmd command, boolean replicated) {
        String rid = "co:" + command.coordinatorId + "-cid:" + command.amoCommand.address().toString() +
                "-" + curConfig.configNum() + "-Abort";
        // generate local proposing `PaxosRequest`
        if (!replicated) {
            if (isDuplicateRequest(rid, command.amoCommand.sequenceNum())) {
                return;
            }
            addToPaxosRequestMap(rid, command.amoCommand.sequenceNum());
            sendPaxosRequest(rid, command);
            return;
        }
        if (isRequestProcessed(rid, command.amoCommand.sequenceNum())) {
            return;
        }
        markRequestDone(rid, command.amoCommand.sequenceNum());
        lockSet.clear();

        Set<Address> to = curConfig.groupInfo().get(command.coordinatorId).getLeft();
        broadcast(new TxAbortCommitMsg(groupId, command.amoCommand), to);
        curTxResult = null;
        txInProgress = false;
        txProgressMap.clear();
        lockSet.clear();
    }

    private void processTxAbortCommitCmd(TxAbortCommitCmd command, boolean replicated) {
        String rid = command.amoCommand.address().toString() + "-" + command.configNum() + "-abort";
        // generate local proposing `PaxosRequest`
        if (!replicated) {
            if (isDuplicateRequest(rid, command.amoCommand.sequenceNum())) {
                return;
            }
            addToPaxosRequestMap(rid, command.amoCommand.sequenceNum());
            sendPaxosRequest(rid, command);
            return;
        }
        if (isRequestProcessed(rid, command.amoCommand.sequenceNum())) {
            return;
        }
        markRequestDone(rid, command.amoCommand.sequenceNum());

        //printTxLogInfo( "aborting " + rid);
        AMOCommand amoCommand = command.amoCommand;
        String sid = amoCommand.address().toString() + "-" + curConfig.configNum();
        if (executedRequests.containsKey(sid)) {
            //printTxLogInfo("removing " + amoCommand + " from executedRequests");
            executedRequests.remove(sid);
        }
        send(new ShardStoreAbortReply(amoCommand.sequenceNum()), amoCommand.address());
        curTxResult = null;
        txInProgress = false;
        txProgressMap.clear();
        lockSet.clear();
    }

    private void processTxCommitCmd(TxCommitCmd command, boolean replicated) {
        String rid = command.amoCommand.address().toString() + "-" + command.configNum();
        // generate local proposing `PaxosRequest`
        if (!replicated) {
            if (isDuplicateRequest(rid, command.amoCommand.sequenceNum())) {
                return;
            }
            addToPaxosRequestMap(rid, command.amoCommand.sequenceNum());
            sendPaxosRequest(rid, command);
            return;
        }
        if (isRequestProcessed(rid, command.amoCommand.sequenceNum())) {
            return;
        }
        markRequestDone(rid, command.amoCommand.sequenceNum());
        AMOCommand amoCommand = command.amoCommand;
        if (executedRequests.containsKey(rid)) {
            executedRequests.get(rid).setRight(SerializationUtils.clone(curTxResult));
        } else {
            executedRequests.put(rid, new MutablePair<>(amoCommand.sequenceNum() ,SerializationUtils.clone(curTxResult) ));
        }
        send(new ShardStoreReply(amoCommand.sequenceNum(), executedRequests.get(rid).getRight()), amoCommand.address());
        //printTxLogInfo("executed txn:" + rid);
        curTxResult = null;
        txInProgress = false;
        txProgressMap.clear();
        lockSet.clear();
    }

    private void processAMOCommand(AMOCommand command, boolean replicated) {
        // generate local proposing `PaxosRequest`
        String id = command.address().toString() ;
        if (!replicated) {
            this.handleMessage(new PaxosRequest(command.address().toString(),command.sequenceNum(),
                    command), paxosAddress);
            return;
        }
        postDecisionProcess(id, command, -1);
    }

    private void handlePaxosDecision(PaxosDecision m, Address sender) {
        if (m.command() instanceof AMOCommand  &&
                curConfig.configNum() != pendingConfig.configNum()) {

            decisionSet.add(SerializationUtils.clone(m));
            return;
        }
        PaxosDecision d = SerializationUtils.clone(m);
        process(d.command(), true);
    }

    private void handlePaxosReply(PaxosReply m, Address sender) {
        if (m.result() instanceof ShardConfig) {
            int newConfig = ((ShardConfig)m.result()).configNum();
            if (txInProgress) return;
            if (curConfig == null ||  newConfig == (curConfig.configNum() + 1)) {
                ShardConfig config = SerializationUtils.clone((ShardConfig) m.result());
                ////printLogInfo(address().toString() + " of group " + groupId + " received config reply:" + m.result());
                process(new NewConfigCmd(config.configNum(), config), false);
            }
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // TODO: your time handlers ...
    private void onQueryTimer(QueryTimer t) {
        int seqNumber = (curConfig == null) ? 0: curConfig.configNum();
        int configId = (curConfig == null) ? 0 : curConfig.configNum() +1;
        broadcastToShardMasters(new PaxosRequest(address().toString(), seqNumber, new Query(configId)));
        set(t, QueryTimer.QUERY_RETRY_MILLIS);
    }

    private void onTransferTimer(TransferTimer t) {
        if (transferMessageMap.containsKey(t.messageId()) && pendingConfig.configNum() == t.configNum()) {
            ShardTransferMsg msg = transferMessageMap.get(t.messageId());
            broadcast(msg, pendingConfig.groupInfo().get(msg.receiverGroupId()).getLeft() );
            set(t, TransferTimer.TRANSFER_RETRY_MILLIS);
        }
    }
    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // TODO: add utils here ...
    private boolean isPhase1Done() {
        for(Integer gid: txProgressMap.keySet()) {
            if (txProgressMap.get(gid).getRight() != 2) {
                return false;
            }
        }
        return true;
    }

    private boolean isPhase2Done() {
        for(Integer gid: txProgressMap.keySet()) {
            if (txProgressMap.get(gid).getRight() != 12) {
                return false;
            }
        }
        return true;
    }
    private boolean hasFollowersAborted() {
        for(Integer gid: txProgressMap.keySet()) {
            if (gid == groupId) continue;
            if ( txProgressMap.get(gid).getRight() > 1) {
                //printTxLogInfo(gid + " did not abort yet");
                return false;
            }
        }
        return true;
    }

    private boolean hasFollowersCommitted() {
        for(Integer gid: txProgressMap.keySet()) {
            if (gid == groupId) continue;
            if ( txProgressMap.get(gid).getRight() != 12) {
                return false;
            }
        }
        return true;
    }
    private boolean isDuplicateRequest(String id, Integer seqNum){
        if (paxosRequestMap.containsKey(id)) {
            MutablePair<Integer, Boolean> p = paxosRequestMap.get(id);
            Integer curSeqNum = p.getLeft();
            return curSeqNum > seqNum ||
                    (curSeqNum.intValue() == seqNum.intValue() && p.getRight());
        }
        return false;
    }

    private void addToPaxosRequestMap(String id, Integer seqNum) {
        if (paxosRequestMap.containsKey(id)) {
            MutablePair<Integer, Boolean> p = paxosRequestMap.get(id);
            p.setLeft(seqNum);
            p.setRight(false);
        } else {
            MutablePair<Integer, Boolean> p = new MutablePair<>(seqNum, false);
            paxosRequestMap.put(id, p);
        }
    }

    private void markRequestDone(String id, Integer seqNum) {
        if (paxosRequestMap.containsKey(id)) {
            MutablePair<Integer, Boolean> p = paxosRequestMap.get(id);
            if (p.getLeft().intValue() == seqNum.intValue()) {
                p.setRight(true);
            }
        } else {
            ////printLogInfo("error for id:" + id + " sequence num: "+ seqNum);
        }
    }

    private boolean isRequestProcessed(String id, Integer seqNum) {
        if (paxosRequestMap.containsKey(id)) {
            MutablePair<Integer, Boolean> p = paxosRequestMap.get(id);
            if (p.getLeft().intValue() == seqNum.intValue()) {
                return p.getRight();
            }
        }
        return false;
    }
    private void makeShardMap(ShardConfig config) {
        pendingShardMap.clear();
        transferMap.clear();
        for(int gid:config.groupInfo().keySet()) {
            if (config.groupInfo().get(gid) == null) continue;
            for(int s:config.groupInfo().get(gid).getRight()){
                pendingShardMap.put(s, gid);
            }
        }

    }

    private boolean hasExecutedTransfers() {
        if (transferMap.size() == 0) return true;

        for(int s: transferMap.keySet()) {
            if (!transferMap.get(s)) {
                ////printLogInfo("group " + groupId + " not executed shardid " + s);
                return false;
            }
        }
        return true;
    }
    private Map<Integer, HashSet<Integer>> getOutgoingGroupShardMap() {
        Map<Integer, HashSet<Integer>> outgoingGroupShardMap = new HashMap<>();
        for(int shardId: curShardMap.keySet()) {
            if (curShardMap.get(shardId) != groupId)  continue;

            Integer gid = pendingShardMap.get(shardId);
            if (gid != groupId) {
                transferMap.put(shardId, false);
                if (!outgoingGroupShardMap.containsKey(gid)){
                    outgoingGroupShardMap.put(gid, new HashSet<>());
                }
                Set<Integer> set = outgoingGroupShardMap.get(gid);
                set.add(shardId);
            }
        }
        return outgoingGroupShardMap;
    }

    private void getIncomingGroupShardMap() {
        //Map<Integer,List<Integer>> incomingGroupShardMap = new HashMap<>();
        for(int shardId:pendingShardMap.keySet()) {
            if (pendingShardMap.get(shardId) != groupId) continue;

            Integer gid = curShardMap.get(shardId);
            if (gid == null || gid != groupId) {
                transferMap.put(shardId, false);
                /*if (!incomingGroupShardMap.containsKey(groupId)){
                    incomingGroupShardMap.put(groupId, new ArrayList<>());
                }
                List<Integer> lst = incomingGroupShardMap.get(groupId);
                lst.add(shardId);*/
            }
        }
    }

    public interface ShardServerCommand extends Command {
    }

    @Data
    public static final class NewConfigCmd implements ShardStoreServer.ShardServerCommand {
        private final int sequenceNum;
        private final ShardConfig config;
    }

    @Data
    public static final class ShardMoveCmd implements ShardStoreServer.ShardServerCommand {
        private final int sequenceNum;
        private final int configNum;
        private final int senderGroupId;
        private final HashMap<Integer, AMOApplication> storeMap;
        private final HashSet<Integer> shardSet;
        private final Integer msgId;
    }

    @Data
    public static final class ShardMoveAckCmd implements ShardStoreServer.ShardServerCommand {
        private final int sequenceNum;
        private final int configNum;
        private final int senderGroupId;
        private final HashSet<Integer> shardSet;
    }

    @Data
    public static final class TxPhase1Cmd implements ShardStoreServer.ShardServerCommand {
        private final int coordinatorId;
        private final int configNum;
        private final AMOCommand amoCommand;
        private final HashSet<Integer> shardsToLock;
    }

    @Data
    public static final class TxPhase2Cmd implements ShardStoreServer.ShardServerCommand {
        private final int coordinatorId;
        private final int configNum;
        private final AMOCommand amoCommand;
    }

    @Data
    public static final class TxReadyCmd implements ShardStoreServer.ShardServerCommand {
        private final int coordinatorId;
        private final int configNum;
        private final AMOCommand amoCommand;
    }

    @Data
    public static final class TxAbortCmd implements ShardStoreServer.ShardServerCommand {
        private final int coordinatorId;
        private final int configNum;
        private final AMOCommand amoCommand;
    }

    @Data
    public static final class TxCommitCmd implements ShardStoreServer.ShardServerCommand {
        private final int coordinatorId;
        private final int configNum;
        private final AMOCommand amoCommand;
    }

    @Data
    public static final class TxAbortCommitCmd implements ShardStoreServer.ShardServerCommand {
        private final int coordinatorId;
        private final int configNum;
        private final AMOCommand amoCommand;
    }

    @Data
    public static final class ClientCmd implements ShardStoreServer.ShardServerCommand {
        private final int configNum;
        private final AMOCommand amoCommand;
    }

    public void printLogInfo(String s) {
        //LOG.info(s);
    }

    public void printTxLogInfo(String s) {
        //LOG.info(s);
    }
    /*


    private void removeIncomingGroup(int gid) {
        List<Integer> lst = incomingGroupShardMap.get(gid);
        if (lst == null || lst.size() == 0) return;

        clearShards(incomingGroupShardMap.get(gid));
        incomingGroupShardMap.remove(gid);
    }

    private void removeOutgoingGroup(int gid) {
        List<Integer> lst = outgoingGroupShardMap.get(gid);
        if (lst == null || lst.size() == 0) return;

        clearShards(outgoingGroupShardMap.get(gid));
        outgoingGroupShardMap.remove(gid);
    }

    private void clearOutgoingGroupShardMap() {
        if (outgoingGroupShardMap.size() == 0) return;
        for(int gid: outgoingGroupShardMap.keySet()) {
            clearShards(outgoingGroupShardMap.get(gid));
        }
        outgoingGroupShardMap.clear();
    }

    private void clearIncomingGroupShardMap() {
        if (incomingGroupShardMap.size() == 0) return;

        for(int gid: incomingGroupShardMap.keySet()) {
            clearShards(incomingGroupShardMap.get(gid));
        }
        incomingGroupShardMap.clear();
    }

    private void clearShards(List<Integer> shards) {
        ListIterator<Integer> itr = shards.listIterator();
        while(itr.hasNext()) {
            itr.remove();
        }
    }
    */

}