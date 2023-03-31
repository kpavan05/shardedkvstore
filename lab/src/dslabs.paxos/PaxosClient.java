package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.java.Log;

import static dslabs.paxos.ClientTimer.CLIENT_RETRY_MILLIS;

@Log
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
    private final Address[] servers;

    //TODO: declare fields for your implementation ...
    private PaxosRequest paxosRequest;
    private PaxosReply paxosReply;
    private int sequenceNumber;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosClient(Address address, Address[] servers) {
        super(address);
        this.servers = servers;
        this.sequenceNumber = -1;
        this.paxosRequest = null;
        this.paxosReply = null;
    }

    @Override
    public synchronized void init() {
        // No need to initialize
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command operation) {
        // TODO: send command ...
        this.sequenceNumber += 1;
        AMOCommand amoCommand = new AMOCommand(operation, this.address(), this.sequenceNumber);
        this.paxosRequest = new PaxosRequest(this.address().toString(), this.sequenceNumber, amoCommand);
        for (Address server : this.servers)
            send(this.paxosRequest, server);
        set(new ClientTimer(this.sequenceNumber), CLIENT_RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
        // TODO: check result available ...
        return this.paxosReply != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // TODO: get result ...
        while (this.paxosReply == null)
            wait();
        Result result = ((AMOResult) this.paxosReply.result()).result();
        this.paxosReply = null;
        return result;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        // TODO: handle paxos server reply ...
        if (paxosRequest != null && m.sequenceNum() == this.paxosRequest.sequenceNum()) {
            this.paxosRequest = null;
            this.paxosReply = m;
            notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // TODO: handle client request timeout ...
        if (paxosRequest != null && t.sequenceNum() == this.paxosRequest.sequenceNum()) {
            for (Address server : this.servers)
                send(this.paxosRequest, server);
            set(t, CLIENT_RETRY_MILLIS);
        }
    }
}
