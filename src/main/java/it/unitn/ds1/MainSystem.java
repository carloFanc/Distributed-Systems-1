package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.Random;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import it.unitn.ds1.Client.ReplicasGroupMsg;
import it.unitn.ds1.Client.ChangeScheduleMsg;
import it.unitn.ds1.Client.SendRandomRequest;
import it.unitn.ds1.Replica.CrashMsg;
import it.unitn.ds1.Replica.CrashType;
import it.unitn.ds1.Replica.JoinGroupMsg;
import it.unitn.ds1.Replica.ElectionMsg;
import it.unitn.ds1.Replica.ElectionReplicaHistory;

public class MainSystem {
	final static int N_REPLICAS = 10;
	final static int N_CLIENTS = 3; // at least 2 clients to test concurrency
	final static int START_COORDINATOR = 0;
	static Random rnd;

	public static void main(String[] args) {
		rnd = new Random();
		
		//create the log file
		try {
			File file = new File("log.txt");
			if (file.createNewFile()) {
				System.out.println("File created: " + file.getName());
			} else {
				System.out.println("File already exists.");
			}
			FileWriter fileWriter = new FileWriter("log.txt", false);
			fileWriter.write("");
			fileWriter.close();
		} catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

		// Create the 'quorumbasedsystem' actor system
		final ActorSystem system = ActorSystem.create("quorumbasedsystem");

		// Create replicas and put them in a list
		List<ActorRef> replicas = new ArrayList<>();
		for (int i = 0; i < N_REPLICAS; i++)
			replicas.add(system.actorOf(Replica.props(i, 0, i == START_COORDINATOR), "replica" + i));

		// Ensure that no one can modify the group 
		replicas = Collections.unmodifiableList(replicas);
		// Send the group member list to everyone in the group 
		JoinGroupMsg join = new JoinGroupMsg(replicas, START_COORDINATOR);
		for (ActorRef peer : replicas)
			peer.tell(join, ActorRef.noSender());

		// Start election in a random replica
		// List<ElectionReplicaHistory> historyZero = new ArrayList<>();
		// replicas.get(rnd.nextInt(N_REPLICAS)).tell(new ElectionMsg(historyZero), ActorRef.noSender());

		// Create the clients and put them in a list
		List<ActorRef> clients = new ArrayList<>();
		for (int i=0; i<N_CLIENTS; i++)
			clients.add(system.actorOf(Client.props(i), "client" + i));


		// --- Base Case with Random Read and Write Requests --
		// Send the replicas group to each client
		ReplicasGroupMsg group = new ReplicasGroupMsg(replicas);
		for (ActorRef client: clients)
			client.tell(group, ActorRef.noSender());

		//---------------------------------------

		// --- Simple Coordinator Crash (detected probabilistically between heartbeat and updateTimeout) ---

		/*waitKey();
		for (ActorRef client: clients)
			client.tell(new ChangeScheduleMsg(.5f), ActorRef.noSender());
		replicas.get(START_COORDINATOR).tell(new CrashMsg(CrashType.Simple), ActorRef.noSender());*/

		// --- Simple Coordinator Crash detected with updateTimeout ---

		/*waitKey();
		for (ActorRef client: clients)
			client.tell(new ChangeScheduleMsg(1f), ActorRef.noSender());
		replicas.get(START_COORDINATOR).tell(new CrashMsg(CrashType.Simple), ActorRef.noSender());*/

		// --- Crash with missing HeartBeat ---

		/*waitKey();
		for (ActorRef client: clients)
			client.tell(new ChangeScheduleMsg(.0f), ActorRef.noSender());
		replicas.get(START_COORDINATOR).tell(new CrashMsg(CrashType.Simple), ActorRef.noSender());*/

		// --- Crash after the coordinator sends an UpdateMsg, before the replicas ack ---
		/*waitKey();
		replicas.get(START_COORDINATOR).tell(new CrashMsg(CrashType.AfterUpdate, 0), ActorRef.noSender());*/

		// --- Crash after sending only half of writeOks ---
		waitKey();
		for (ActorRef client: clients)
			client.tell(new ChangeScheduleMsg(1f), ActorRef.noSender());
		replicas.get(START_COORDINATOR).tell(new CrashMsg(CrashType.CrashAfterHalfWriteOk), ActorRef.noSender());


		// --- Concurrent WriteRequest ---
		/*System.out.println("Concurrent Update");
		clients.get(0).tell(new SendRandomRequest(1f, 0), ActorRef.noSender());
		clients.get(1).tell(new SendRandomRequest(1f, 0), ActorRef.noSender());
		clients.get(2).tell(new SendRandomRequest(1f, 0), ActorRef.noSender());
		System.out.println("Concurrent Reads");
		clients.get(0).tell(new SendRandomRequest(0f, 150), ActorRef.noSender());
		clients.get(1).tell(new SendRandomRequest(0f, 165), ActorRef.noSender());
		clients.get(2).tell(new SendRandomRequest(0f, 160), ActorRef.noSender());*/

		// --- Crash the winner during election ---
		/*waitKey();
		replicas.get(START_COORDINATOR).tell(new CrashMsg(CrashType.Simple), ActorRef.noSender());
		clients.get(0).tell(new SendRandomRequest(1f, 0), ActorRef.noSender());
		replicas.get(1).tell(new CrashMsg(CrashType.WinnerCrash), ActorRef.noSender());*/

		// --- Replica Crash during election after sending an ack ---
		/*waitKey();
		replicas.get(START_COORDINATOR).tell(new CrashMsg(CrashType.Simple), ActorRef.noSender());
		clients.get(0).tell(new SendRandomRequest(1f, 0), ActorRef.noSender());
		replicas.get(5).tell(new CrashMsg(CrashType.ElectionAfterAck), ActorRef.noSender());*/

		waitKey();
		try {
			System.out.println(">>> Press ENTER to exit <<<");
			System.in.read();
		}
		catch (IOException ioe) {}
		system.terminate();
	}

	private static void waitKey(){
		try {
			System.out.println(">>> Press to next Crash <<<");
			System.in.read();
		}
		catch (IOException ioe) {}
	}
}