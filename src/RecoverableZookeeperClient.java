
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper.States;

public class RecoverableZookeeperClient {
	/**
	 * HBase Perf Test Main Class
	 * 
	 * @param args
	 * @throws IOException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws NoSuchMethodException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public static void main(String[] args) throws IOException, NoSuchMethodException, InvocationTargetException,
			IllegalAccessException, InterruptedException, KeeperException {
		if (args.length < 1) {
			System.out.println("specify zkQuorun");
		}
		String zkQuorum=args[0];
		System.out.println("Zookeeper Quorum " + zkQuorum);
		// Create Zookeeper Connection
		Configuration conf = getHbaseConfig(
				zkQuorum,
				"2181");
		HConnection connection = HConnectionManager.getConnection(conf);
		ZooKeeperWatcher connectionZK = getZooKeeperWatcher(connection);
		
		// Get Zookeeper Connection details
		System.out.println("ZooKeeperWatcher= 0x" + Integer.toHexString(connectionZK.hashCode()));
		System.out.println(
				"getRecoverableZooKeeper= 0x" + Integer.toHexString(connectionZK.getRecoverableZooKeeper().hashCode()));
		System.out.println("session=" + Long.toHexString(connectionZK.getRecoverableZooKeeper().getSessionId()));

		System.out.println("Before using zkw state=" + connectionZK.getRecoverableZooKeeper().getState());

		// Sample Zookeeper operations
		byte[] expectedData = new byte[] { 1, 2, 3 };
		ZKUtil.createWithParents(connectionZK, "/l1/l2/l3/l4/testCreateWithParents", expectedData);
		byte[] data = ZKUtil.getData(connectionZK, "/l1/l2/l3/l4/testCreateWithParents");
		System.out.println(" Create & get data " + new String(data));
		ZKUtil.deleteNodeRecursively(connectionZK, "/l1");

		ZKUtil.createWithParents(connectionZK, "/testCreateWithParents", expectedData);
		data = ZKUtil.getData(connectionZK, "/testCreateWithParents");
		System.out.println(" Create & get data " + new String(data));
		ZKUtil.deleteNodeRecursively(connectionZK, "/testCreateWithParents");

		/*
		 * // provoke session expiration by doing something with ZK try {
		 * connectionZK.getRecoverableZooKeeper().getZooKeeper().exists( "/1/1",
		 * false); } catch (KeeperException ignored) { }
		 */

		// Check that the old ZK connection is closed, means we did expire
		States state = connectionZK.getRecoverableZooKeeper().getState();
		System.out.println("After using zkw state=" + state);
		System.out.println("session=" + Long.toHexString(connectionZK.getRecoverableZooKeeper().getSessionId()));

		// It's asynchronous, so we may have to wait a little...
		final long limit1 = System.currentTimeMillis() + 3000;
		while (System.currentTimeMillis() < limit1 && state != States.CLOSED) {
			state = connectionZK.getRecoverableZooKeeper().getState();
		}
		System.out.println("After using zkw loop=" + state);
		System.out.println("ZooKeeper should have timed out");
		System.out.println("session=" + Long.toHexString(connectionZK.getRecoverableZooKeeper().getSessionId()));

		// It's surprising but sometimes we can still be in connected state.
		// As it's known (even if not understood) we don't make the the test
		// fail
		// for this reason.)
		// Assert.assertTrue("state=" + state, state == States.CLOSED);

		// Check that the client recovered
		ZooKeeperWatcher newConnectionZK = getZooKeeperWatcher(connection);

		States state2 = newConnectionZK.getRecoverableZooKeeper().getState();
		System.out.println("After new get state=" + state2);

		// As it's an asynchronous event we may got the same ZKW, if it's not
		// yet invalidated. Hence this loop.
		final long limit2 = System.currentTimeMillis() + 3000;
		while (System.currentTimeMillis() < limit2 && state2 != States.CONNECTED && state2 != States.CONNECTING) {

			newConnectionZK = getZooKeeperWatcher(connection);
			state2 = newConnectionZK.getRecoverableZooKeeper().getState();
		}
		System.out.println("After new get state loop=" + state2);
		System.out.println("Connected " + (state2 == States.CONNECTED || state2 == States.CONNECTING));

		connection.close();

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
