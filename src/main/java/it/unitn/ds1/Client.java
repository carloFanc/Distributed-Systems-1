package it.unitn.ds1;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import it.unitn.ds1.Replica.ReadResponse;
import scala.concurrent.duration.Duration;

public class Client extends AbstractActor {
	private final int id;
	private Random rnd = new Random();
	private ArrayList<ActorRef> replicas = new ArrayList<>();
	private Cancellable schedule;

	public Client(int id) {
		this.id = id;  
	}

	static public Props props(int id) {
		return Props.create(Client.class, () -> new Client(id));
	}

	//------------------------------
	@Override
	public void preStart() {
		// schedule = getContext().system().scheduler().scheduleWithFixedDelay(
		// 		Duration.create(2, TimeUnit.SECONDS),         	// when to start generating messages
		// 		Duration.create(2, TimeUnit.SECONDS),         	// how frequently generate them
		// 		getSelf(),										// destination actor reference
		// 		new SendRandomRequest(.5f, 0),						// the message to send
		// 		getContext().system().dispatcher(),           	// system dispatcher
		// 		getSelf());										// source of the message 
	}

	/** Message to let clients know about replicas */
	public static class ReplicasGroupMsg implements Serializable {
		public final List<ActorRef> group;   // an array of group members
		public ReplicasGroupMsg(List<ActorRef> group) {
			this.group = Collections.unmodifiableList(new ArrayList<ActorRef>(group));
		}
	}
	
	/** Message to request a read */
	public static class ReadRequest implements Serializable {
		public final int clientId;
		public ReadRequest(int clientId) {
			this.clientId = clientId;
		}
	}

	/** Message to request an update */
	public static class UpdRequest implements Serializable {
		public final int v;
		public UpdRequest(int v) {
			this.v = v;
		}
	} 

	/** Message to send a randomly chosen request */
	public static class SendRandomRequest implements Serializable {
		public final float p; // propability to update instead of read
		public final int d; // dealy in ms
		public SendRandomRequest(float p, int d){
			this.p = p;
			this.d = d;
		}
	}

	/** Message to schedule a sendRandomRequest */
	public static class ChangeScheduleMsg implements Serializable {
		public final float p; // propability to update instead of read
		public ChangeScheduleMsg(float p){
			this.p = p;
		}
	}

	//-------------------------------------
	//##region actor logic

	/** Adds replica to the group.*/
	private void onReplicasGroupMsg(ReplicasGroupMsg msg) {
		for (ActorRef r: msg.group) {
			replicas.add(r);
		}
	}

	/** Response containing the requested replica value */
	public void onReadResponse(ReadResponse resp) {
		System.out.println("Client "+this.id+" read value: "+resp.v+" update: "+ resp.u.toString()); 
		writeToFile("Client " + this.id + " read value: " + resp.v); 
	}

	/** Randomly sends a write or read request from the client to a randomly chosen replica.*/
	public void onSendRandomRequest(SendRandomRequest req) {
		int randId = rnd.nextInt(replicas.size());

		if (rnd.nextDouble() > req.p) {
			randSleep(req.d);
			System.out.println("Client "+this.id+" read req for replica "+ randId);
			writeToFile("Client "+this.id+" read request for replica "+ randId);
			replicas.get(randId).tell(new ReadRequest(this.id), getSelf());
		} else {
			int v = rnd.nextInt(10);
			System.out.println("Client "+this.id+" update req for replica "+randId+" value: "+v);
			writeToFile("Client "+this.id+" update request for replica "+randId+" value: "+v);
			replicas.get(randId).tell(new UpdRequest(v), getSelf());
		}
	}

	/** Message that for convenience schedules a SendRandomRequest*/
	public void onChangeSchedule(ChangeScheduleMsg msg){ 
		if (schedule != null)
			schedule.cancel();
		schedule = getContext().system().scheduler().scheduleWithFixedDelay(
				Duration.create(2, TimeUnit.SECONDS),         	// when to start generating messages
				Duration.create(2, TimeUnit.SECONDS),         	// how frequently generate them
				getSelf(),										// destination actor reference
				new SendRandomRequest(msg.p, 0),				// the message to send
				getContext().system().dispatcher(),           	// system dispatcher
				getSelf());										// source of the message 
	}
 
	//--------------------------------------------

	 
	private void randSleep(int delay){
		try {
			Thread.sleep(rnd.nextInt(10)+delay); 
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
				.match(ReplicasGroupMsg.class,  this::onReplicasGroupMsg)
				.match(ReadResponse.class, this::onReadResponse)
				.match(SendRandomRequest.class, this::onSendRandomRequest)
				.match(ChangeScheduleMsg.class, this::onChangeSchedule)
				.build();
	}
}
