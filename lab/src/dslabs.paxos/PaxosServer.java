package dslabs.paxos;

import com.google.common.base.Objects;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    static final int INITIAL_BALLOT_NUMBER = 0;
    private final Address[] servers;

    // TODO: declare fields for your implementation ...
    private final Address rootAddress;
    private final AMOApplication<Application> app;

    /* for garbage collection log update */
    private int nextClearSlot;

    /* for execution detection, whether the request executed before (< this.slot_out) */
    private final HashMap<String, Pair<HashMap<Integer, Integer>, Integer>> ExecutedRecords = new HashMap<>();

    /* for replica */
    @Data
    private static final class PaxosLogSlot implements Serializable {
        private final PaxosLogSlotStatus status;
        private final PaxosRequest paxosRequest;
    }

    private final HashMap<Integer, PaxosLogSlot> PaxosLog = new HashMap<>();
    // Current Client Requests in Log: PaxosRequest -> slots
    private final HashMap<String, HashMap<PaxosRequest, HashSet<Integer>>> RequestSlotRecords = new HashMap<>();
    private int slotIn, slotOut;  // slotIn: Next To Fill; slotOut: Next To Decide

    /* for leader */
    private boolean active;
    private Ballot leaderBallot;

    /* for acceptors */
    private Ballot acceptorBallot;
    private final HashMap<Integer, Pvalue> PvalueRecords = new HashMap<>();

    // client request - in-order generation assumption
    private final HashMap<String, PaxosRequest> ClientRequest = new HashMap<>();

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        // TODO: wrap app inside AMOApplication ...
        this.rootAddress = null;
        this.app = new AMOApplication<>(app);
    }

    // for Project 5
    public PaxosServer(Address address, Address[] servers, Address rootAddress) {
        super(address);
        this.servers = servers;

        // Your code here...
        this.app = null;
        this.rootAddress = rootAddress;
    }

    @Override
    public void init() {
        // TODO: initialize fields ...
        this.nextClearSlot = 1;
        this.slotIn = 1;
        this.slotOut = 1;

        this.active = false;
        this.leaderBallot = new Ballot(INITIAL_BALLOT_NUMBER + 1, this.address());

        this.acceptorBallot = new Ballot(INITIAL_BALLOT_NUMBER, this.address());
        this.issuePhase1Request();
    }


    /* -------------------------------------------------------------------------
        Interface Methods

        Be sure to implement the following methods correctly. The test code uses
        them to check correctness more efficiently.
       -----------------------------------------------------------------------*/

    /**
     * Return the status of a given slot in the servers's local log.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's status
     */
    public PaxosLogSlotStatus status(int logSlotNum) {
        // TODO: return slot status in logSlotNum ...
        if (this.PaxosLog.containsKey(logSlotNum))
            return this.PaxosLog.get(logSlotNum).status(); // nextClear - slotIn
        if (logSlotNum < this.nextClearSlot)
            return PaxosLogSlotStatus.CLEARED;
        assert logSlotNum >= this.slotIn : String.format("Slot Status Not Expected! (CLEAR: %s, OUT: %s, IN: %s)", this.nextClearSlot, this.slotOut, this.slotIn);
        return PaxosLogSlotStatus.EMPTY;
    }

    /**
     * Return the command associated with a given slot in the server's local
     * log. If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
     * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. If
     * clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this
     * method should unwrap them before returning.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's contents or {@code null}
     */
    public Command command(int logSlotNum) {
        // TODO: return command assigned in logSlotNum ...
        if (!this.PaxosLog.containsKey(logSlotNum))
            return null;
        PaxosLogSlot paxosLogSlot = this.PaxosLog.get(logSlotNum);
        assert paxosLogSlot.status != PaxosLogSlotStatus.CLEARED : "Slot Statue As Cleared!";
        if (paxosLogSlot.status() == PaxosLogSlotStatus.EMPTY)
            return null;
        Command command = paxosLogSlot.paxosRequest().command();
        assert command instanceof AMOCommand : "Command Not As AMOCommand";
        return ((AMOCommand) command).command();
    }

    /**
     * Return the index of the first non-cleared slot in the server's local
     * log.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     */
    public int firstNonCleared() {
        // TODO: return first slot that is not garbage collected yet ...
        return this.nextClearSlot;
    }

    /**
     * Return the index of the last non-empty slot in the server's local log. If
     * there are no non-empty slots in the log, this method should return 0.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     */
    public int lastNonEmpty() {
        // TODO: return last non empty slot in paxos log ...
        return this.slotIn - 1;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // TODO: handle paxos request ...

        // handle readonly request
        if (m.command().readOnly()) {
            if (this.app != null) {
                send(new PaxosReply(m.id(), m.sequenceNum(), this.app.executeReadOnly(m.command())), sender);
            }
            return;
        }

        // send back decisions - slot_out check
        if (this.whetherRequestComplete(m)) {
            if (this.app!= null) {
                this.sendRequestReply(m);
            }
            return;
        }

        // check whether in log already
        if (this.whetherRequestInProposal(m)) {
            return;
        }

        String id = m.id();
        // ignore the outdated client requests (in-order request generation assumption)
        if (!this.ClientRequest.containsKey(id) || this.ClientRequest.get(id).sequenceNum() < m.sequenceNum()) {
            this.ClientRequest.put(id, m);
            if (this.active)
                this.issuePhase2Request();
        }
    }

    // TODO: your message handlers ...


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // TODO: your time handlers ...


    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // TODO: add utils here ...
    private boolean whetherRequestComplete(PaxosRequest paxosRequest) {
        String id = paxosRequest.id();
        return (this.ExecutedRecords.containsKey(id) && this.ExecutedRecords.get(id).getRight() >= paxosRequest.sequenceNum());
    }

    private boolean whetherRequestInProposal(PaxosRequest paxosRequest) {
        String id = paxosRequest.id();
        return (this.RequestSlotRecords.containsKey(id) && this.RequestSlotRecords.get(id).containsKey(paxosRequest));
    }

    private void updateExcutedRecord(int slotNum, PaxosRequest paxosRequest) {
        String id = paxosRequest.id();
        int sequenceNum = paxosRequest.sequenceNum();
        if (!this.ExecutedRecords.containsKey(id))
            this.ExecutedRecords.put(id, new MutablePair<>(new HashMap<>(), -1));
        this.ExecutedRecords.get(id).getLeft().putIfAbsent(sequenceNum, slotNum);
        int currentMax = this.ExecutedRecords.get(id).getRight();
        this.ExecutedRecords.get(id).setValue(Math.max(sequenceNum, currentMax));
    }

    private void issuePhase1Request() {
        assert !this.active : "Repeat Issue Scout";

        // single server - the acceptor ballot must be smaller
        HashMap<Integer, Pvalue> pvalues = new HashMap<>();

        for (int i : this.PvalueRecords.keySet()) {
            Pvalue pvalue = this.PvalueRecords.get(i);
            if (pvalues.containsKey(i) && pvalue.ballot().compareTo(pvalues.get(i).ballot()) < 0)
                continue;
            pvalues.put(i, pvalue);
        }

        this.active = true;
        for (Map.Entry<Integer, Pvalue> entry : pvalues.entrySet())
            this.updatePaxosLog(entry.getKey(), entry.getValue().paxosRequest(), PaxosLogSlotStatus.ACCEPTED);
        this.issuePhase2Request();
    }

    private void issuePhase2Request() {
        assert this.active : "Non Active Leader Issue Phase 2";
        // get current request
        for (String id : this.ClientRequest.keySet()) {
            if (!(this.whetherRequestComplete(this.ClientRequest.get(id)) || this.whetherRequestInProposal(this.ClientRequest.get(id))))
                this.updatePaxosLog(this.slotIn, this.ClientRequest.get(id), PaxosLogSlotStatus.ACCEPTED);
        }
        this.ClientRequest.clear();

        // this is a easy version solution, there is no need to think much about empty slot
        HashMap<Integer, PaxosRequest> proposeRequests = new HashMap<>();
        for (int i = this.slotOut; i < this.slotIn; i++) {
            PaxosLogSlot paxosLogSlot = this.PaxosLog.get(i);
            if (paxosLogSlot.status != PaxosLogSlotStatus.EMPTY)
                proposeRequests.put(i, paxosLogSlot.paxosRequest());
        }

        // commit to Paxos log since there is only one server
        for (int i : proposeRequests.keySet())
            this.updatePaxosLog(i, proposeRequests.get(i), PaxosLogSlotStatus.CHOSEN);
    }

    private void updateRequestRecord(int slotNum, PaxosRequest paxosRequest) {
        assert paxosRequest != null : "Slot Request As Null - Update Record";
        String id = paxosRequest.id();
        if (!this.RequestSlotRecords.containsKey(id))
            this.RequestSlotRecords.put(id, new HashMap<>());
        if (!this.RequestSlotRecords.get(id).containsKey(paxosRequest))
            this.RequestSlotRecords.get(id).put(paxosRequest, new HashSet<>());
        this.RequestSlotRecords.get(id).get(paxosRequest).add(slotNum);
    }

    private void removeRequestRecord(int slotNum, PaxosRequest paxosRequest) {
        assert paxosRequest != null : "Slot Request As Null - Remove Record";
        String id = paxosRequest.id();
        assert this.RequestSlotRecords.containsKey(id) : "Request Record Not Contain " + id;
        assert this.RequestSlotRecords.get(id).containsKey(paxosRequest) : "Request Record Not Contain Corresponding Paxos - " + id + "\n" + paxosRequest.toString();
        
        this.RequestSlotRecords.get(id).get(paxosRequest).remove(slotNum);
        if (this.RequestSlotRecords.get(id).get(paxosRequest).size() == 0)
            this.RequestSlotRecords.get(id).remove(paxosRequest);
        if (this.RequestSlotRecords.get(id).size() == 0)
            this.RequestSlotRecords.remove(id);
    }

    private void updatePaxosLog(int slotNum, PaxosRequest paxosRequest, PaxosLogSlotStatus status) {
        assert paxosRequest != null : "Update Paxos Log With Request As Null";
        if (slotNum < this.slotOut) {
            assert slotNum < this.nextClearSlot || status != PaxosLogSlotStatus.CHOSEN || Objects
                    .equal(this.PaxosLog.get(slotNum).paxosRequest(), paxosRequest) : "Try To Assign Different Paxos Log, Smaller Than Slot Out";
            return;
        }

        if (slotNum >= this.slotIn) {
            for (int i = this.slotIn; i < slotNum + 1; i++) {
                this.PaxosLog.put(i, new PaxosLogSlot(PaxosLogSlotStatus.EMPTY, null));
            }
            this.slotIn = slotNum + 1;
        }

        if (this.PaxosLog.get(slotNum).status == PaxosLogSlotStatus.CHOSEN) {
            assert Objects.equal(this.PaxosLog.get(slotNum).paxosRequest(), paxosRequest) : "Try To Assign Different Paxos Log, Already Chosen";
            return;
        }

        if (this.PaxosLog.get(slotNum).status == PaxosLogSlotStatus.ACCEPTED && !Objects.equal(this.PaxosLog.get(slotNum).paxosRequest(), paxosRequest))
            this.removeRequestRecord(slotNum, this.PaxosLog.get(slotNum).paxosRequest());

        this.PvalueRecords.put(slotNum, new Pvalue(slotNum, this.acceptorBallot, paxosRequest));
        this.PaxosLog.put(slotNum, new PaxosLogSlot(status, paxosRequest));
        this.updateRequestRecord(slotNum, paxosRequest);

        while (this.slotOut < this.slotIn) {
            if (this.PaxosLog.get(this.slotOut).status != PaxosLogSlotStatus.CHOSEN)
                break;
            PaxosRequest request = this.PaxosLog.get(this.slotOut).paxosRequest();
            this.sendRequestReply(request);
            this.updateExcutedRecord(this.slotOut, request);
            this.slotOut += 1;
        }

        // perform garbage collection
        for (int i = this.nextClearSlot; i < this.slotOut; i++) {
            assert this.PaxosLog.get(i).status() == PaxosLogSlotStatus.CHOSEN : "Slot Status As " + this.PaxosLog.get(i).status().toString() + ", Not Chosen - Garbage Clean";
            this.removeRequestRecord(i, this.PaxosLog.get(i).paxosRequest());
            this.PaxosLog.remove(i);
            this.PvalueRecords.remove(i);
        }
        this.nextClearSlot = this.slotOut;
    }

    private void sendRequestReply(PaxosRequest paxosRequest) {
        if (this.app != null) {
            if (!(paxosRequest.command() instanceof AMOCommand))
                throw new IllegalArgumentException();
            AMOCommand amoCommand = (AMOCommand) paxosRequest.command();
            send(new PaxosReply(paxosRequest.id(), paxosRequest.sequenceNum(), this.app.execute(amoCommand)), amoCommand.address());
        } else {
            assert this.rootAddress != null : "Sending Decision with Root Addrees AS NULL";
            this.handleMessage(new PaxosDecision(paxosRequest.id(), paxosRequest.sequenceNum(), paxosRequest.command()), this.rootAddress);
        }
    }
}