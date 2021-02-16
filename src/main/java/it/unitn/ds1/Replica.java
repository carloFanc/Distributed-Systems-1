package it.unitn.ds1;

import akka.actor.*;
import it.unitn.ds1.Client.ReadRequest;
import it.unitn.ds1.Client.UpdRequest; 
import scala.concurrent.duration.Duration;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit; 

public class Replica extends AbstractActor {

	private final int MAX_TIME = 10;
	private final int TIMEOUT_TIME = 400;
	private final int ACK_TIME = 200;
	private final int ELECTION_TIMEOUT = 2000;
	private final int HEARTBEAT_TIMEOUT = 3000;

	private final int id; 
	private ActorRef coordinatorRef; 
	private int v;
	private boolean isCoordinator = false;
	private Random rnd;
	private List<ActorRef> peers = new ArrayList<>();	
	private CrashType nextCrash; 
	private int[] vc;

	public class Update { 
		public final int e;
		public final int i;
		public Update(int e, int i) { 
			this.e = e;
			this.i = i;
		}
		public String toString(){
			return "<" + e + ">:<" + i + ">";
		}
	}

	//current update
	private Update lastUpdate;
	//history update
	private Update historyUpdate;
	//incompleteUpdate
	private Update incompleteUpdate; 
	//incompleteValue
	private int incompleteValue;
	//history of replica
	private List<Update> history = new ArrayList<>();
	//hashmap to save <e,i> and n ack 
	private Map<Update, Integer> quorum = new HashMap<Update, Integer>();

	private List<UpdateMsg> updateMsgQueue = new ArrayList<>();

	// If the coordinator is not heard for too long.
	private Cancellable hbTimeout;	
	// If the UPDATE message is not received on time after forwarding an update REQUEST to the coordinator
	private Cancellable updateTimeout;
	// If the WRITEOK message is not received on time after the UPDATE.
	private Cancellable wokTimeout;	
	// Election ack timeout. 
	private Cancellable electionAckTimeout;
	// Election timeout. 
	private Cancellable electionTimeout;
	// HeartBeat timeout. 
	private Cancellable heartBeat;

	//-------------------------------------------------
	public Replica(int id, int v, boolean isCoordinator) {
		this.id = id;
		this.v = v;
		this.isCoordinator = isCoordinator;
		rnd = new Random();
		this.lastUpdate = new Update(0,0); 
		this.historyUpdate = new Update(0,0); 
		this.nextCrash = CrashType.None;

		this.incompleteUpdate = null; 
		this.incompleteValue = 0;

		this.hbTimeout = null;
		this.updateTimeout = null;
		this.wokTimeout = null;
		this.electionAckTimeout = null;
	}

	static public Props props(int id, int v, boolean isCoordinator) {
		return Props.create(Replica.class, () -> new Replica(id,v,isCoordinator));
	}

	//Crash types
	public enum CrashType {
		None,
		Simple,
		AfterUpdate,			// Crash after the coordinator responds with an UpdateMsg
		CrashAfterHalfWriteOk,	// Crash after sending only half of writeOks
		WinnerCrash,			// Crash the winner during election
		ElectionAfterAck,		// Replica Crash during election after sending an ack
	}

	//Class to represent histories during election phase composed by history and the id of the replica
	public class ElectionReplicaHistory { 
		public final Update lastUpdate;
		public final int id;
		public ElectionReplicaHistory(Update lastUpdate, int id) {
			this.lastUpdate = lastUpdate;
			this.id = id;
		}
	}
	//--------------------------------------------------------------
	
	/** Message informing all participants about its peers. */
	public static class JoinGroupMsg implements Serializable {
		public final List<ActorRef> group;
		public final int coordinatorId;
		public JoinGroupMsg(List<ActorRef> group, int coordinatorId) {
			this.group = Collections.unmodifiableList(new ArrayList<>(group));
			this.coordinatorId = coordinatorId;
		}
	}
	
	/** Start HeartBeat message from coordinator. */
	public static class CoordinatorSendingHeartBeatMsg implements Serializable {}

	/** Message informing replicas that coordinator has not crashed yet. */
	public static class HeartBeatMsg implements Serializable {}

	/** Message containing this replica value for ReadRequests. */
	public static class ReadResponse implements Serializable {
		public final int v;
		public final Update u;
		public ReadResponse(int v, Update u) {
			this.v = v;
			this.u = u;
		}
	}

	/** Message of update request from a replica to the coordinator. */
	public static class UpdateRequest implements Serializable {
		public final int v;
		public UpdateRequest(int v){
			this.v = v;
		}
	}

	/** Upper class implementing vector clock */
	public static abstract class VectorClockMsg implements Serializable {
		public final int[] vc;       // vector clock
		protected VectorClockMsg(int[] vc){
			this.vc = new int[vc.length];
			for (int i=0; i<vc.length; i++) this.vc[i] = vc[i];
		}
	}

	/** Message of update from coordinator to all other replicas. */
	public static class UpdateMsg extends VectorClockMsg implements Serializable {
		public final int v;
		public final Update u;
		public UpdateMsg(int v, Update u, int[] vc){
			super(vc);
			this.v = v;	
			this.u = u;
		}
	}
	
	/** Acknowledgment of update. */
	public static class Ack extends VectorClockMsg implements Serializable {
		public final int v;
		public final Update u;
		public Ack(int v, Update u, int[] vc){
			super(vc);
			this.v = v;	
			this.u = u;
		}
	}
	
	/** Message confirming the update. */
	public static class WriteOk implements Serializable {
		public final int v;
		public final Update u;
		public WriteOk(int v, Update u){
			this.v = v;	
			this.u = u;
		}
	}
	
	/** Message to make replicas crash. */
	public static class CrashMsg implements Serializable {
		public final CrashType nextCrash;
		public CrashMsg(CrashType nextCrash) {
			this.nextCrash = nextCrash;
		}
	}

	/** Message to elect a new coordinator. */
	public static class ElectionMsg implements Serializable {
		public final List<ElectionReplicaHistory> histories;
		public final int electionNumber;
		public final int nextId;
		public final int TTL;
		public final String msg;
		public ElectionMsg(List<ElectionReplicaHistory> histories,int nextId ,int electionNumber, int TTL, String msg){
			this.histories = Collections.unmodifiableList(new ArrayList<ElectionReplicaHistory>(histories));
			this.electionNumber = electionNumber;
			this.nextId = nextId;
			this.msg = msg;
			this.TTL = TTL;
		}
	}
	
	/** Message to start a new election */
	public static class StartElectionMsg implements Serializable {
		public final int electionNumber;
		public final String msg;
		public StartElectionMsg(int electionNumber, String msg){
			this.electionNumber = electionNumber;
			this.msg = msg;
		}
	}
	
	/** Acknowledgment message for election. */
	public static class ElectionAck implements Serializable {
		public final int electionNumber;
		public ElectionAck(int electionNumber){
			this.electionNumber = electionNumber;
		}
	}

	/** Message of the new coordinator after an election. */
	public static class SynchronizationMsg implements Serializable {
		public final Update update;
		public final int v;
		public SynchronizationMsg(Update update, int v){
			this.update = update;
			this.v = v;
		}
	}

	/** Message to finish any uncompleted update. */
	public static class CompleteUpdateMsg implements Serializable {
		public final Update incompleteUpdate;
		public final int incompleteValue;
		public CompleteUpdateMsg(Update incompleteUpdate, int incompleteValue){
			this.incompleteUpdate = incompleteUpdate; 
			this.incompleteValue = incompleteValue;
		}
	}
	//-------------------------------------
	//##region actor logic

	/** Replicas know each other with the initialization of the vc */
	private void onJoinGroupMsg(JoinGroupMsg msg) {
		peers.addAll(msg.group);
		coordinatorRef = peers.get(msg.coordinatorId);
		this.vc = new int[this.peers.size()];
	}

	/** Schedule heartbeat message. */
	public void preStart() {
		if (heartBeat != null)
			heartBeat.cancel();
		heartBeat = getContext().system().scheduler().scheduleWithFixedDelay(
				Duration.create(1, TimeUnit.SECONDS),        // when to start generating messages
				Duration.create(2, TimeUnit.SECONDS),        // how frequently generate them
				getSelf(),                                   // destination actor reference
				new CoordinatorSendingHeartBeatMsg(),        // the message to send
				getContext().system().dispatcher(),          // system dispatcher
				getSelf()                                    // source of the message 
				);
	}

	/** Broadcast heartbeat message. */
	private void onCoordinatorSendingHeartBeatMsg(CoordinatorSendingHeartBeatMsg msg){
		if(this.isCoordinator) {
			multicast(new HeartBeatMsg(), peers);
		}
	}

	/** Deleting (or triggering) HeartBeat timeout. */
	private void onHeartBeatMsg(HeartBeatMsg msg){
		if(hbTimeout != null)
			hbTimeout.cancel();
		
		hbTimeout = Timeout(HEARTBEAT_TIMEOUT, "HeartBeat Timeout by "+this.id);
	}

	/** Process read request from the client. */
	private void onReadRequest(ReadRequest req) { 
		System.out.println("Client " + req.clientId + " read req to " + this.id);
		writeToFile("Client " + req.clientId + " read req to " + this.id);
		unicast(new ReadResponse(this.v, lastUpdate), sender());
	}

	/** Forward update request to every replica.*/
	private void onWriteRequest(UpdRequest req){
		if(this.isCoordinator){
			getSelf().tell(new UpdateRequest(req.v), getSelf());
		} else {
			unicast(new UpdateRequest(req.v), coordinatorRef);
			if(updateTimeout != null) {
				updateTimeout.cancel();
			}
			updateTimeout = Timeout(TIMEOUT_TIME, "Update Timeout by "+this.id);
		}
	}

	/** Coordinator updates the value and broadcasts the update message. */
	private void onUpdateRequest(UpdateRequest req) {
		if(this.isCoordinator){
			this.vc[id]++;
			//System.out.println("coordinator is "+this.id);
			this.historyUpdate = new Update(this.historyUpdate.e, this.historyUpdate.i + 1);
			multicast(new UpdateMsg(req.v, historyUpdate, this.vc), peers);
			quorum.put(this.historyUpdate, 1);

			if(nextCrash == CrashType.AfterUpdate){
				crash();
				System.out.println("\tCOORDINATOR CRASHED AFTER UPDATE");
				writeToFile("\tCOORDINATOR CRASHED AFTER UPDATE");
			}
		}
	}

	/** Replica acknowledges the update. */
	private void onUpdateMsg(UpdateMsg msg){

		if(updateTimeout != null)
			updateTimeout.cancel();

		if (!areConcurrent(this.vc, msg.vc)){
			msg.vc[id]++;
			updateLocalClock(msg.vc);
			unicast(new Ack(msg.v, msg.u, this.vc), getSender());

			incompleteUpdate = msg.u;
			incompleteValue = msg.v;

			//System.out.println("Id: "+this.id+"\tincompleteUpdate: "+incompleteUpdate.toString()+ "\tincValue: "+incompleteValue);
			writeToFile("Id: "+this.id+"\tincompleteUpdate: "+incompleteUpdate.toString()+ "\tincValue: "+incompleteValue);
			
			if(wokTimeout != null)
				wokTimeout.cancel();
			wokTimeout = Timeout(TIMEOUT_TIME, "WriteOk Timeout by: "+this.id);
		}
		else {
			//System.out.println("id: "+this.id+" CONCURRENT with value: "+msg.v);
			//writeToFile("id: "+this.id+" CONCURRENT with value: "+msg.v);
			this.updateMsgQueue.add(msg);
		} 
	}

	/** Acknowledgment with quorum selection. */
	private void onAck(Ack msg) {
		// System.out.println("UpdateAck - value: "+msg.v);
		if(quorum.get(msg.u) != null){
			quorum.put(msg.u, quorum.get(msg.u) + 1);
			if(quorum.get(msg.u) >= peers.size() / 2 + 1){
				int i = 0;
				this.vc = new int[peers.size()];
				for(ActorRef peer : peers) {
					if (i == peers.size()/2 && nextCrash == CrashType.CrashAfterHalfWriteOk) {
						crash();
						System.out.println("\tCOORDINATOR CRASHED AFTER SOME WRITEOK");
						writeToFile("\tCOORDINATOR CRASHED AFTER SOME WRITEOK");
						break;
					}

					unicast(new WriteOk(msg.v, msg.u), peer);
					i++;
				}
				quorum.remove(msg.u);
			}
		}
	}

	/** Update writeOk */
	private void onWriteOk(WriteOk msg){
		if(wokTimeout != null) 
			wokTimeout.cancel();

		this.v = msg.v;
		this.lastUpdate = msg.u;
		this.vc = new int[peers.size()];
		history.add(lastUpdate);
		System.out.println("Replica " + this.id + " update " + this.lastUpdate.toString() + " value:" + this.v);
		writeToFile("Replica " + this.id + " update " + this.lastUpdate.toString() + " value:" + this.v);

		incompleteUpdate = null;
		incompleteValue = 0;

		UpdateMsg m = null;
		do {
			m = findDeliverable(); // find concurrent updates
			if (m != null){
				unicast(new Ack(m.v, m.u, m.vc), getSender()); // if any, send the ack to the coordinator

				if(wokTimeout != null)
					wokTimeout.cancel();
				wokTimeout = Timeout(TIMEOUT_TIME, "WriteOk Timeout by: " + this.id);
			}
		} while (m != null/* && !areConcurrent(this.vc, m.vc)*/);
	}
	
	/** Message that starts the election and triggers the timeout election for possible crashes  */
	private void onStartElectionMsg(StartElectionMsg msg){
		System.out.println("\tElection Started");
		writeToFile("\tElection Started");

		if (electionTimeout != null)
			electionTimeout.cancel();
		electionTimeout = getContext().system().scheduler().scheduleOnce( // retry the election
				Duration.create(ELECTION_TIMEOUT, TimeUnit.MILLISECONDS),
				getSelf(),
				new StartElectionMsg(rnd.nextInt(), "timeoutElection by " + this.id),
				getContext().system().dispatcher(),
				getSelf()
				);

		onElectionMsg(new ElectionMsg(new ArrayList<ElectionReplicaHistory>(), this.id, msg.electionNumber, 0, msg.msg));
	}
	
	/** Message to elect a new coordinator. */
	private void onElectionMsg(ElectionMsg msg){
		if (!getSender().equals(getSelf())){
			unicast(new ElectionAck(msg.electionNumber), getSender());
			if (nextCrash == CrashType.ElectionAfterAck){
				crash();
				System.out.println("\n\tREPLICA "+this.id+" CRASHED AFTER SENDING ACK\n");
				writeToFile("\n\tREPLICA "+this.id+" CRASHED AFTER SENDING ACK\n");
				return;
			}
		}

		if (msg.TTL > peers.size()*2 || this.isCoordinator)
			return;

		//if (msg.msg != null)
			//System.out.println(msg.msg);

		int winnerId = -1;
		int maxE = -1;
		int maxI = -1;

		for(ElectionReplicaHistory h : msg.histories) {
			int e = h.lastUpdate.e;
			int i = h.lastUpdate.i;
			if (maxE == e && maxI == i && winnerId > h.id)
				winnerId = h.id;
			else if (maxE < e || (maxE == e && maxI < i)){
				maxE = e;
				maxI = i;
				winnerId = h.id;
			}
		}

		System.out.println("Election n. "+msg.electionNumber+" in "+this.id+"\t winner is: "+ winnerId+ "\t length: "+msg.histories.size()+"\t precId: "+msg.nextId+"\t TTL: "+msg.TTL);
		writeToFile("Election n. "+msg.electionNumber+" in "+this.id+"\t winner is: "+ winnerId+ "\t length: "+msg.histories.size()+"\t precId: "+msg.nextId+"\t TTL: "+msg.TTL);
		
		if (winnerId == this.id && !getSender().equals(getSelf())){
			if (nextCrash == CrashType.WinnerCrash){
				System.out.println("\n\tWINNER CRASH\n");
				writeToFile("\n\tWINNER CRASH\n");
				crash();
				return;
			}
			System.out.println("\tReplica "+this.id+" is the new coordinator");
			writeToFile("Replica "+this.id+" is the new coordinator");
			broadcast(new SynchronizationMsg(this.lastUpdate, this.v), peers);
			this.isCoordinator = true;
		}
		else {

			List<ElectionReplicaHistory> personalHistories = new ArrayList<ElectionReplicaHistory>(msg.histories);
			// if (!getSender().equals(getSelf()))
			personalHistories.add(new ElectionReplicaHistory(this.lastUpdate, this.id));

			int nextId = (msg.nextId + 1) % peers.size();

			ElectionMsg msgNew = new ElectionMsg(personalHistories, nextId, msg.electionNumber, msg.TTL+1, null);

			electionMode();

			unicast(msgNew, peers.get(nextId));

			if (electionAckTimeout != null)
				electionAckTimeout.cancel();
			electionAckTimeout = getContext().system().scheduler().scheduleOnce( // retry with the next id if ack isn't recevied
					Duration.create(ACK_TIME, TimeUnit.MILLISECONDS),
					getSelf(),
					new ElectionMsg(personalHistories, nextId, msg.electionNumber, msg.TTL+1, "ElectionAckTimeout in "+this.id + "n. "+msg.electionNumber),
					getContext().system().dispatcher(),
					getSelf()
					);
		}
	}
	
	/** Acknowledgment of election message */
	private void onElectionAck(ElectionAck ack) {
		if (electionAckTimeout != null){
			electionAckTimeout.cancel();
			//System.out.println("Election Ack "+this.id+ "\t"+ack.electionNumber);
			//writeToFile("Election Ack "+this.id+ "\t"+ack.electionNumber);
		}
	}
	
	/** Message of the new coordinator for synchronizing all replicas. 
	    If this replica has an incomplete update, sends CompleteUpdateMsg to the coordinator. */
	private void onSynchronization(SynchronizationMsg msg){
		if (electionTimeout != null)
			electionTimeout.cancel();
		preStart();
		if (incompleteUpdate != null){
			// System.out.println("Replica: "+this.id+" INCOMPLETE: "+this.incompleteUpdate.toString()+" VALUE: "+this.incompleteValue);
			unicast(new CompleteUpdateMsg(this.incompleteUpdate,this.incompleteValue), getSender());
		}

		updateMsgQueue.clear();

		this.lastUpdate = new Update(msg.update.e + 1, 0);
		this.historyUpdate = new Update(msg.update.e + 1, 0);

		this.coordinatorRef = getSender();
		getContext().become(createReceive());
	}

	/** Message with an incomplete update to finish*/
	private void onCompleteUpdate(CompleteUpdateMsg msg) {
		broadcast(new WriteOk(msg.incompleteValue, msg.incompleteUpdate), peers);
	}

	/** Message in which the chosen crash type is assigned*/
	private void onCrashMsg(CrashMsg msg){
		switch (msg.nextCrash) {
		case Simple:
			crash();
			break;
		default:
			nextCrash = msg.nextCrash;
			break;
		}
	}

	//--------------------------------------------
	// #region Function Helpers

	/** Crash event function */
	private void crash(){
		getContext().become(crashed());
		if (heartBeat != null)
			heartBeat.cancel();
	}

	/** Election event function*/
	private void electionMode(){
		getContext().become(election());
		if (heartBeat != null)
			heartBeat.cancel();
		if (hbTimeout != null)
			hbTimeout.cancel();
	}

	/** Function for determining the concurrency of two vector clocks */
	private static boolean areConcurrent(final int[] vc, final int[] vc2){
		boolean a = false, b = false;
		for (int i = 0; i < vc.length; i++)
			if(vc[i] < vc2[i]){
				a = true;
				break;
			}
		for (int i = 0; i < vc2.length; i++)
			if(vc2[i] < vc[i]){
				b = true;
				break;
			}
		return a && b;
	}
	
	/** Find a message in the queue that can be delivered now;
        if found, remove it from the queue and return it      */
	private UpdateMsg findDeliverable() {   
		Iterator<UpdateMsg> I = updateMsgQueue.iterator();
		while (I.hasNext()) {
			UpdateMsg m = I.next();
			if (!areConcurrent(this.vc, m.vc)) {
				I.remove();
				return m;
			}
		}
		return null;        // nothing can be delivered right now
	}

	private void multicast(Serializable msg, List<ActorRef> group){
		for (ActorRef actor : group) {
			if(!actor.equals(getSelf())){
				randSleep();
				actor.tell(msg, getSelf());
			}
		}
	}

	private void broadcast(Serializable msg, List<ActorRef> group){
		for (ActorRef actor : group) {
			randSleep();
			actor.tell(msg, getSelf());
		}
	}

	private void unicast(Serializable msg, ActorRef destination){
		randSleep();
		destination.tell(msg, getSelf());
	}
	
	/** Update of the local clock */
	private void updateLocalClock(int[] vc) {
		for (int i=0; i<vc.length; i++)
			this.vc[i] = (vc[i] > this.vc[i]) ? vc[i] : this.vc[i];
	} 

	/** Timeout for coordinator crash detection  */
	private Cancellable Timeout(int ms, String msg){
		int electionNumber = rnd.nextInt();
		return getContext().system().scheduler().scheduleOnce(
				Duration.create(ms, TimeUnit.MILLISECONDS),
				getSelf(),
				new StartElectionMsg(electionNumber, msg),
				getContext().system().dispatcher(),
				getSelf());
	}

	private void randSleep(){
		try {
			Thread.sleep(rnd.nextInt(MAX_TIME)); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void writeToFile(String text){
		try (FileWriter fileWriter = new FileWriter("log.txt", true)) {
			fileWriter.append(text + "\n"); 
		} catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(JoinGroupMsg.class, this::onJoinGroupMsg)
				.match(CoordinatorSendingHeartBeatMsg.class, this::onCoordinatorSendingHeartBeatMsg)
				.match(HeartBeatMsg.class,  this::onHeartBeatMsg)
				.match(UpdateRequest.class, this::onUpdateRequest)
				.match(ReadRequest.class, this::onReadRequest)
				.match(UpdRequest.class, this::onWriteRequest)
				.match(UpdateMsg.class, this::onUpdateMsg)
				.match(Ack.class, this::onAck)
				.match(WriteOk.class, this::onWriteOk)
				.match(SynchronizationMsg.class, this::onSynchronization)
				.match(CompleteUpdateMsg.class, this::onCompleteUpdate)
				.match(CrashMsg.class, this::onCrashMsg)
				.match(StartElectionMsg.class, this::onStartElectionMsg)
				.match(ElectionMsg.class, this::onElectionMsg)
				.match(ElectionAck.class, this::onElectionAck)
				.build();
	}

	final AbstractActor.Receive crashed() {
		return receiveBuilder()
				.matchAny(msg -> {})
				.build();
	}

	final AbstractActor.Receive election() {
		return receiveBuilder()
				.match(JoinGroupMsg.class, this::onJoinGroupMsg)
				.match(CoordinatorSendingHeartBeatMsg.class, this::onCoordinatorSendingHeartBeatMsg)
				.match(HeartBeatMsg.class,  this::onHeartBeatMsg) 
				.match(WriteOk.class, this::onWriteOk)
				.match(SynchronizationMsg.class, this::onSynchronization)
				.match(CompleteUpdateMsg.class, this::onCompleteUpdate)
				.match(StartElectionMsg.class, this::onStartElectionMsg)
				.match(ElectionMsg.class, this::onElectionMsg)
				.match(ElectionAck.class, this::onElectionAck)
				.matchAny(msg -> {})
				.build();
	}
}