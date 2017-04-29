import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZkConnectMultiple {

	public ZooKeeper connect(String host) throws Exception {
       final CountDownLatch connSignal = new CountDownLatch(1);
    	ZooKeeper zk = new ZooKeeper(host, 60000, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == KeeperState.SyncConnected) {
                    connSignal.countDown();
                }
            }
        });
        connSignal.await();
        return zk;
    }

    public void close(ZooKeeper zk ) throws InterruptedException {
        zk.close();
    }

    public void createNode(ZooKeeper zk , String path, byte[] data) throws Exception
    {
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void updateNode(ZooKeeper zk , String path, byte[] data) throws Exception
    {
        zk.setData(path, data, zk.exists(path, true).getVersion());
    }

    public void deleteNode(ZooKeeper zk , Pattern nodePattern) throws Exception
    {
    	List<String> zNodesList = zk.getChildren("/", true);
    	for (int i=0; i<zNodesList.size(); i++) {
    		String zNode = zNodesList.get(i);
    		Matcher m = nodePattern.matcher(zNode);
    		if (m.find( )) {
    			 zk.delete('/' + zNode,0);
    			
    		}
    	}
    }

    public static void main (String args[]) throws Exception
    {
    	if (args.length < 5) {
    		System.out.println("specify zkQuorun zkNodeName numConnections testtype testSpeed ");
    	}
    	
    	String zkQuorum=args[0];
    	System.out.println("Zookeeper Quorum " + zkQuorum);
    	String nodeName=args[1];
    	System.out.println("Zk Node prefix " + nodeName);
    	int numConn = Integer.parseInt(args[2]);
    	System.out.println("Num ZK connections " + numConn);
    	String test=args[3];
    	System.out.println("test type " + test);
    	String testspeed=args[4];
    	System.out.println("test speed " + testspeed);
    	long now = 0L;
    	long latency = 0L;
    	
        ZkConnectMultiple connector = new ZkConnectMultiple();
        ArrayList<ZooKeeper> zklist = new ArrayList<ZooKeeper>();
        System.out.println("Creating " +  numConn + " zookeeper connections");
        for (int i=0; i<numConn; i++ ) {
        	ZooKeeper zk = connector.connect(zkQuorum);
        	zklist.add(zk);
        	if(testspeed.equals("slow")) {
        	    Thread.sleep(5);
        	}
        }
        
    	if (test.equals("delete")) {
    		System.out.println("Deleting  " +  numConn + " zookeeper nodes " + nodeName);
    		Pattern nodePattern = Pattern.compile(nodeName);
    		for (int i=0; i<numConn; i++) {
    			now = System.nanoTime();
    			connector.deleteNode(zklist.get(i) ,nodePattern);
	            latency = System.nanoTime() - now;
	            System.out.println(" deleteNode latency " + latency);
	        	if(testspeed.equals("slow")) {
	        	    Thread.sleep(5);
	        	}
    			
    		}
    	}
    	if (test.equals("create")) {
    		System.out.println("Creating  " +  numConn + " zookeeper nodes " + nodeName);
	        for (int i=0; i<numConn; i++) {
	            String newNode = "/" + nodeName+i;
	            now = System.nanoTime();
	            connector.createNode(zklist.get(i) , newNode, new Date().toString().getBytes());
	            latency = System.nanoTime() - now;
	            System.out.println(" createNode latency " + latency);
	        	if(testspeed.equals("slow")) {
	        	    Thread.sleep(5);
	        	}
	        }
    	}
        while(true) {
	        for (int i=0; i<numConn; i++) {
	            String newNode = "/" + nodeName+i;
	            now = System.nanoTime();
	            List<String> zNodes = zklist.get(i).getChildren("/", true);
		        latency = System.nanoTime() - now;
		        System.out.println(" getChildren latency " + latency);
	            /*
		        for (String zNode: zNodes)
		        {
		           System.out.println("ChildrenNode " + zNode);  
		        }
		        */
		        now = System.nanoTime();
		        byte[] data = zklist.get(i).getData(newNode, true, zklist.get(i).exists(newNode, true));
		        latency = System.nanoTime() - now;
		        System.out.println(" getData latency " + latency);
		        /*
		        for ( byte dataPoint : data)
		        {
		            System.out.print ((char)dataPoint);
		        }
		        */
		        now = System.nanoTime();
		        connector.updateNode(zklist.get(i), newNode, new Date().toString().getBytes());
		        latency = System.nanoTime() - now;
		        System.out.println(" setData latency " + latency);
		        
		        now = System.nanoTime();
		        data = zklist.get(i).getData(newNode, true, zklist.get(i).exists(newNode, true));
		        latency = System.nanoTime() - now;
		        System.out.println("getData after setData latency " + latency);
		        /*
		        for ( byte dataPoint : data)
		        {
		            System.out.print ((char)dataPoint);
		        }
		        */
	        	if(testspeed.equals("slow")) {
	        	    Thread.sleep(5);
	        	}
	        }
	        Thread.sleep(5);
        }
    }

}
