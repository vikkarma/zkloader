import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

public class RecoverableZookeeperMultiClient {

	static class RecoverableZKTest implements Runnable {
		private String zkQuorum;
		private int znodeId;
		private int numGetSet;
		boolean infinite;

		public RecoverableZKTest(String zkQuorum, int znodeId, int numGetSet, boolean infinite) {
			this.zkQuorum = zkQuorum;
			this.znodeId = znodeId;
			this.numGetSet = numGetSet;
			this.infinite = infinite;
		}

		@Override
		public void run() {
			// Create Zookeeper Connection
			Configuration conf = getHbaseConfig(zkQuorum, "2181");
			HConnection connection = null;
			try {
				connection = HConnectionManager.getConnection(conf);
				ZooKeeperWatcher connectionZK = null;
				connectionZK = getZooKeeperWatcher(connection);
				// Get Zookeeper Connection details
				System.out.println("ZooKeeperWatcher= 0x" + Integer.toHexString(connectionZK.hashCode()));
				System.out.println("getRecoverableZooKeeper= 0x"
						+ Integer.toHexString(connectionZK.getRecoverableZooKeeper().hashCode()));
				System.out
						.println("session=" + Long.toHexString(connectionZK.getRecoverableZooKeeper().getSessionId()));

				System.out.println("Before using zkw state=" + connectionZK.getRecoverableZooKeeper().getState());

				// GetSet Zookeeper operations
				byte[] data;
				String zNode = "/l1" + znodeId + "/l2/l3/l4/testCreateWithParents" + znodeId;
				System.out.println(" Create znode " + zNode);
				ZKUtil.createWithParents(connectionZK, zNode, new Date().toString().getBytes());
				do {
					for (int i = 0; i < numGetSet; i++) {
						data = ZKUtil.getData(connectionZK, zNode);
						System.out.println(" Get znode " + zNode + " data " + data);
						ZKUtil.setData(connectionZK, zNode, new Date().toString().getBytes());
						System.out.println(" set znode " + zNode);
						System.out.println("zkw state=" + connectionZK.getRecoverableZooKeeper().getState());
						System.out.println(
								"session=" + Long.toHexString(connectionZK.getRecoverableZooKeeper().getSessionId()));
					}
				} while (infinite);
				System.out.println("Deleting /l1" + znodeId);
				ZKUtil.deleteNodeRecursively(connectionZK, "/l1" + znodeId);
				connection.close();
			} catch (IOException | KeeperException | NoSuchMethodException | InvocationTargetException
					| IllegalAccessException e) {
				System.out.println("" + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	/**
	 * HBase Perf Test Main Class
	 * 
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		if (args.length < 4) {
			System.out.println("specify zkQuorun numThreads numGetSet infinite");
		}
		String zkQuorum = args[0];
		int numThreads = Integer.parseInt(args[1]);
		int numGetSet = Integer.parseInt(args[2]);
		boolean infinite = Boolean.parseBoolean(args[3]);
		System.out.println("Zookeeper Quorum " + zkQuorum);

		ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
		for (int i = 0; i < numThreads; i++) {
			executorService.execute(new RecoverableZKTest(zkQuorum, i, numGetSet, infinite));
		}
		executorService.awaitTermination(1, TimeUnit.HOURS);
	}

	/**
	 * Initialize Hbase connection configuration
	 * 
	 * @param hbaseHost
	 * @param hbasePort
	 * @return
	 */
	protected static Configuration getHbaseConfig(String hbaseHost, String hbasePort) {
		// Initialize configuration for normal Htable pools
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", hbaseHost);
		conf.set("hbase.zookeeper.property.clientPort", hbasePort);

		conf.set("hbase.client.retries.number", "4");
		conf.set("hbase.client.pause", "1000");
		conf.set("zookeeper.session.timeout", "60000");
		conf.set("hbase.rpc.timeout", "60000");
		conf.set("zookeeper.recovery.retry", "3");
		conf.set("zookeeper.recovery.retry.intervalmill", "1000");
		return conf;
	}

	private static ZooKeeperWatcher getZooKeeperWatcher(HConnection c)
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Method getterZK = c.getClass().getDeclaredMethod("getKeepAliveZooKeeperWatcher");
		getterZK.setAccessible(true);
		return (ZooKeeperWatcher) getterZK.invoke(c);
	}
}
