import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ZkSocketConnectTest {
	private static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

	public ZooKeeper connect(String host) throws Exception {
       final CountDownLatch connSignal = new CountDownLatch(1);
    	ZooKeeper zk = new ZooKeeper(host, 60000, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == KeeperState.SyncConnected) {
                	System.out.println("" + dateFormat.format(new Date()) + " connected ..");
                    connSignal.countDown();
                }
            }
        });
        connSignal.await();
        return zk;
    }

    public static void main (String args[]) throws Exception
    {
    	if (args.length < 3) {
    		System.out.println("specify zkQuorum numConnections connectSleep");
    	}
    	
    	String zkQuorum=args[0];
    	System.out.println("Zookeeper Quorum " + zkQuorum);
    	int numConn = Integer.parseInt(args[1]);
    	System.out.println("Num ZK connections " + numConn);
    	int connectSleep=Integer.parseInt(args[2]);
    	System.out.println("Connect for " + connectSleep + " ms");
    	
        ZkSocketConnectTest connector = new ZkSocketConnectTest();        
        System.out.println("Creating " +  numConn + " zookeeper connections");
        while (true) {
	        for (int i=0; i<numConn; i++ ) {
	        	ZooKeeper zk = connector.connect(zkQuorum);
	       	    Thread.sleep(connectSleep);
	       	    zk.close();
	       	 System.out.println("" + dateFormat.format(new Date()) + " closed ..");
	        }
        }
    }

}
