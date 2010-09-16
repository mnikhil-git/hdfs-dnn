/**
 * To Do
 * 
 * handle multi-block files - order blocks
 * 	add hierarchical locking
 * 	add support for permissions
 * 	add support for leasing
 * 	add support for block generations?
 * 
 * finish namenode actions:
 * 	TEST::implement delete
 * 	TEST::implement mkdirs
 * 	implement rename
 * 
 * finish datanode actions:
 * 	error reporting
 * 
 * 
 * 
 * store this kind of stuff in namespaceTable for each file:
 * 
 * Path path
 * long length
 * boolean isdir
 * short block_replication
 * long blocksize
 * long modification_time
 * long access_time
 * FsPermission permission
 * String owner
 * String group
 * 
 */

package org.apache.hadoop.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Random;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;

public class DistributedNamenodeProxy implements ClientProtocol, DatanodeProtocol {

	private HBaseConfiguration config;
	private HTable namespaceTable;
	private Random rand = new Random();
	private Replicator replicator = new Replicator();
	private HTable blocksTable;
	private HTable datanodesTable;

	private byte[] infoFam = "info".getBytes();
	private byte[] childrenFam = "children".getBytes();
	private byte[] blocksFam = "blocks".getBytes();
	private byte[] datanodesFam = "datanodes".getBytes();
	private byte[] blank = "".getBytes();

	//private HealthProtocol healthServer;
	private long lastCapacity = -1;
	private long lastDfsUsed = -1;
	private long lastRemaining = -1;

	private class Replicator {

		// TODO: make this respect configured replication settings

		private HashSet<String> targets;

		Replicator() {
			targets = new HashSet<String>();
		}


		// rescan the datanodes table to keep the set of datanodes up to date
		// other client code can also help us remove nodes from this set when reporting
		// failed interactions with a datanode
		private void scanDatanodes() throws IOException {
			System.out.println("scanning datanodes table ..");
			targets.clear();
			Scan s = new Scan();
			s.addColumn(infoFam, "remaining".getBytes());

			ResultScanner scanner = datanodesTable.getScanner(s);
			for(Result result : scanner) {
				targets.add(new String(result.getRow()));
			}
		}

		DatanodeInfo[] getReplicationTargets() throws IOException {

			// TODO: periodically scan the datanodes table to find new datanodes
			if(targets.size() == 0)
				scanDatanodes();

			// pick nodes at random
			// TODO: take into account whether a datanode is too full to host another block
			// the old namenode would also have a hard limit on the total number
			// of fs objects it could store 
			int replicationFactor = 1;

			if(targets.size() < replicationFactor)
				throw new IOException("unable to achieve required replication: too few datanodes running");

			HashSet<String> targetSetNames = new HashSet<String>();

			HashSet<DatanodeInfo> targetSet = new HashSet<DatanodeInfo>();
			for(int i=0; i < replicationFactor; i++) {
				int r = rand.nextInt(targets.size());

				Iterator<String> iter = targets.iterator();
				for(int j=0; j < r-1; iter.next());
				String target = iter.next();

				// don't create two replicas on the same target
				while(targetSetNames.contains(target)) {
					r = rand.nextInt(targets.size());
					iter = targets.iterator();
					for(int j=0; j < r-1; iter.next());
					target = iter.next();
				}

				targetSet.add(new DatanodeInfo(new DatanodeID(target)));
			}

			DatanodeInfo[] targetSetArray = targetSet.toArray(new DatanodeInfo[targetSet.size()]);
			return targetSetArray;
		}
	}

	public DistributedNamenodeProxy() throws IOException {
		System.out.println("========= Distributed Name Node Proxy init =========");
		config = new HBaseConfiguration();

		namespaceTable = new HTable(config, "namespace");
		datanodesTable = new HTable(config, "datanodes");
		blocksTable = new HTable(config, "blocks");

		String healthNodeHost = config.get("healthnode");
		if(healthNodeHost == null)
			throw new IOException("error: no healthnode address specified. add one to core-site.xml");

		//InetSocketAddress healthNodeAddr = new InetSocketAddress(healthNodeHost, 9090);


		//healthServer = (HealthProtocol)RPC.getProxy(HealthProtocol.class,
		//		HealthProtocol.versionID, healthNodeAddr, config,
		//	        NetUtils.getSocketFactory(config, HealthProtocol.class));

	}

	@Override
	public void abandonBlock(Block b, String src, String holder)
	throws IOException {
		System.out.println("using abandonBlock");

	}


	/**
	 * helpers 
	 * both of these write to blocksTable and datanodesTable
	 * 
	 * @param host
	 * @param hblocks
	 * @throws IOException
	 */
	private void recordHostBlocks(String host, long[] hblocks) throws IOException {

		if(hblocks.length == 0)
			return;

		Put hostData = new Put(host.getBytes());
		for(int i=0; i < hblocks.length; i++)
			hostData.add(blocksFam, Long.toString(hblocks[i]).getBytes(), "".getBytes());
		datanodesTable.put(hostData);

		ArrayList<Put> blockData = new ArrayList<Put>();
		for(int i=0; i < hblocks.length; i++) {
			Put block = new Put(Long.toString(hblocks[i]).getBytes());
			block.add(datanodesFam, host.getBytes(), blank);
			blockData.add(block);
		}

		blocksTable.put(blockData);
	}

	private void recordBlockHosts(byte[] blockIDBytes, DatanodeInfo[] hosts) throws IOException {

		if(hosts.length == 0)
			return;

		Put blockData = new Put(blockIDBytes);
		for(int i=0; i < hosts.length; i++)
			blockData.add(datanodesFam, hosts[i].name.getBytes(), blank);
		blocksTable.put(blockData);

		ArrayList<Put> hostData = new ArrayList<Put>();
		for(int i=0; i < hosts.length; i++) {
			Put host = new Put(hosts[i].name.getBytes());
			host.add(blocksFam, blockIDBytes, blank);
			hostData.add(host);
		}

		datanodesTable.put(hostData);
	}

	/**
	 * The client would like to obtain an additional block for the indicated
	 * filename (which is being written-to).  Return an array that consists
	 * of the block, plus a set of machines.  The first on this list should
	 * be where the client writes data.  Subsequent items in the list must
	 * be provided in the connection to the first datanode.
	 *
	 * Make sure the previous blocks have been reported by datanodes and
	 * are replicated.  Will return an empty 2-elt array if we want the
	 * client to "try again later".
	 * @throws IOException 
	 */
	@Override
	public LocatedBlock addBlock(String src, String clientName)
	throws IOException {
		System.out.println("using addBlock " + src + " " + clientName);

		// create new blocks on data nodes
		long blockID = rand.nextLong();
		byte[] blockIDBytes = Long.toString(blockID).getBytes();

		Block b = new Block(blockID, 0, 0);

		// choose a set of nodes on which to replicate block
		DatanodeInfo[] targets = replicator.getReplicationTargets(); 

		// TODO: get a lease to the first
		// TODO: can we record all this in the namespace table?
		// i.e. do we ever need to lookup blocks without knowing the associated files?
		// blockReceived() doesn't know the file mapping

		// record block to host mapping and vice versa
		recordBlockHosts(blockIDBytes, targets);

		// get the last block ID
		Get blocksGet = new Get(src.getBytes());
		blocksGet.addFamily("blocks".getBytes());

		int blockPos = 0;
		Result blockList = namespaceTable.get(blocksGet);

		if(blockList != null) {
			NavigableMap<byte[],byte[]> blockMap = blockList.getFamilyMap("blocks".getBytes());
			if(blockMap != null) {
				blockPos = blockMap.keySet().size();
			}
		}


		// record file to block mapping
		Put nameData = new Put(src.getBytes());
		nameData.add(blocksFam, (blockPos + "_" + blockID).getBytes(), blank);
		namespaceTable.put(nameData);

		LocatedBlock newBlock = new LocatedBlock(b, targets);
		return newBlock;
	}

	@Override
	public LocatedBlock append(String src, String clientName)
	throws IOException {
		System.out.println("using append");
		return null;
	}

	@Override
	public boolean complete(String src, String clientName) throws IOException {
		System.out.println("using complete");

		// write complete status to namespace?
		// does this just help avoid mutations to existent complete files?

		Get fileGet = new Get(src.getBytes());
		fileGet.addFamily(blocksFam);

		Result fileResult = namespaceTable.get(fileGet);
		NavigableMap<byte[],byte[]> blocks = fileResult.getFamilyMap(blocksFam);

		long fileSize = 0;

		// record final size
		if(blocks != null) { // allow files of length 0
			for(byte[] blockID : blocks.keySet()) {
				Get blockGet = new Get(new String(blockID).split("_")[1].getBytes());
				blockGet.addColumn("info:size".getBytes());

				Result blockResult = blocksTable.get(blockGet);
				fileSize += Long.parseLong(new String(blockResult.getValue(infoFam, "size".getBytes())));
			}
		}

		// write size to namespace table
		Put fileSizePut = new Put(src.getBytes());
		fileSizePut.add(infoFam, "size".getBytes(), Long.toString(fileSize).getBytes());

		namespaceTable.put(fileSizePut);

		return true;
	}

	@Override
	public void create(String src, FsPermission masked, String clientName,
			boolean overwrite, short replication, long blockSize)
	throws IOException {
		System.out.println("using create");

		// verify that parent directories exist
		byte[] parent = getParentPath(src);
		Get parentGet = new Get(parent);
		parentGet.addColumn(infoFam, "isDir".getBytes());
		parentGet.addFamily(childrenFam);

		Result parentResult = namespaceTable.get(parentGet);
		if(parentResult == null || parentResult.isEmpty())
			throw new IOException("parent directory not found: " + new String(parent));

		if(new String(parentResult.getValue(infoFam, "isDir".getBytes())).equals("N"))
			throw new IOException("parent " + src + " is not a directory");
		
		// verify src doesn't exist as a directory already
		if(parentResult.containsColumn(childrenFam, src.getBytes()))
			throw new IOException("file exists: " + src);


		// TODO: check access permissions

		// edit namespace table to create this file

		/*
		 * not yet recorded:
		 * 
		 * long length
		 * long modification_time
		 * long access_time
		 * String owner
		 * String group
		 */

		ArrayList<Put> creates = new ArrayList<Put>();

		Put createRequest = new Put(src.getBytes());
		createRequest.add(infoFam, Bytes.toBytes("create_time"), Bytes.toBytes(Long.toString(System.currentTimeMillis())));
		createRequest.add(infoFam, Bytes.toBytes("replication"), Bytes.toBytes(Short.toString(replication)));
		createRequest.add(infoFam, Bytes.toBytes("blocksize"), Bytes.toBytes(Long.toString(blockSize)));
		createRequest.add(infoFam, Bytes.toBytes("permission"), Bytes.toBytes(masked.toString()));
		createRequest.add(infoFam, Bytes.toBytes("isDir"), Bytes.toBytes("N"));

		creates.add(createRequest);

		// record existence of new file in parent dir now or on complete?
		Put childCreate = new Put(getParentPath(src));
		// TODO: could store that this is a file in the Value
		childCreate.add(childrenFam, src.getBytes(), blank);

		creates.add(childCreate);

		// write info all at once
		namespaceTable.put(creates);

		// TODO: verify replication

		// TODO: grant a lease to the client??
		// GFS grants the lease to the primary datanode, not the client
	}

	@Override
	public boolean delete(String src) throws IOException {
		System.out.println("using delete");

		// NameNode code calls this with recursive=true as default
		return delete(src, true);
	}

	/**
	 * recursively create delete objects for src and all children
	 * 
	 * @param src
	 * @param recursive
	 * @return
	 * @throws IOException 
	 */
	private ArrayList<Delete> getDeletes(byte[] src) throws IOException {
		ArrayList<Delete> deletes = new ArrayList<Delete>();

		Get get = new Get(src);
		get.addFamily(childrenFam);

		Result r = namespaceTable.get(get);
		if(r == null || r.isEmpty())
			return deletes; // ignore

		NavigableMap<byte[],byte[]> children = r.getFamilyMap(childrenFam);
		if(children == null)
			return deletes;

		// delete children
		for(byte[] child: children.keySet()) {
			deletes.addAll(getDeletes(child));
		}

		Delete delete = new Delete(src);
		deletes.add(delete);

		return deletes;
	}

	@Override
	public boolean delete(String src, boolean recursive) throws IOException {
		System.out.println("using delete");
		// check permissions - how?

		byte[] parent = getParentPath(src);

		// determine whether this is a directory
		Get get = new Get(src.getBytes());
		get.addColumn(infoFam, "isDir".getBytes());
		get.addFamily(childrenFam);

		Result r = namespaceTable.get(get);
		if(r == null || r.isEmpty())
			throw new IOException("file or dir not found: " + src);

		NavigableMap<byte[],byte[]> children = r.getFamilyMap(childrenFam);
		String isDir = new String(r.getValue(infoFam, "isDir".getBytes()));

		if(isDir.equals("Y") && !recursive && children.size() > 0)
			throw new IOException("can't delete directory. not empty");


		Delete childDelete = new Delete(parent);
		childDelete.deleteColumn(childrenFam, src.getBytes());

		ArrayList<Delete> deletes = getDeletes(src.getBytes());
		deletes.add(childDelete);

		// delete everything at once
		namespaceTable.delete(deletes);

		// blocks are removed via garbage collection
		// TODO: when to return false?
		return true;
	}

	@Override
	public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action)
	throws IOException {
		System.out.println("using distributedUpgradeProgress");
		return null;
	}

	@Override
	public void finalizeUpgrade() throws IOException {
		System.out.println("using finalizeUpgrade");

	}

	@Override
	public void fsync(String src, String client) throws IOException {
		System.out.println("using fsync");

	}

	@Override
	public LocatedBlocks getBlockLocations(String src, long offset, long length)
	throws IOException {
		System.out.println("using getBlockLocations: " + src + " " + offset + " " + length);

		ArrayList<LocatedBlock> locatedBlocks = new ArrayList<LocatedBlock>();

		// get blocks from namespace table
		Get fileBlocksGet = new Get(src.getBytes());
		fileBlocksGet.addFamily(blocksFam);
		fileBlocksGet.addFamily(infoFam);

		System.out.println("getting blocks for " + src + " from namespace table");
		Result fileBlocks = namespaceTable.get(fileBlocksGet);

		long fileLength = 0;

		byte[] fileSizeBytes = fileBlocks.getValue(infoFam, "size".getBytes());
		if(fileSizeBytes == null)
			throw new IOException("file not found: " + src);

		fileLength = Long.parseLong(new String(fileSizeBytes));

		// TODO: calculate which blocks we need for the offset and length
		// TODO: could store the actual long id in the value of the table
		NavigableMap<byte[],byte[]> IDs = fileBlocks.getFamilyMap(blocksFam);
		if(IDs == null) {
			throw new IOException("file not found: " + src);
		}

		long blockOffset = 0L;
		for(byte[] id : IDs.keySet()) {
			// remove block position indicator
			id = new String(id).split("_")[1].getBytes();

			String blockIDString = new String(id);
			System.out.println("found block: " + blockIDString);

			// lookup host and length information for each block
			// TODO: can we join this data into the namespace table?
			Get blockHostGet = new Get(id);
			blockHostGet.addColumn(datanodesFam);
			blockHostGet.addColumn(infoFam, "size".getBytes());

			System.out.println("getting host data for block ...");
			Result hosts = blocksTable.get(blockHostGet);

			// get block length 
			byte[] blockSizeCell = hosts.getValue(infoFam, "size".getBytes());
			long blockSize = Long.parseLong(Bytes.toString(blockSizeCell));


			// add block info to the list of located blocks
			ArrayList<DatanodeInfo> dni = new ArrayList<DatanodeInfo>();
			for(byte[] host : hosts.getFamilyMap(datanodesFam).keySet()) {
				System.out.println("got host: " + new String(host) + " for block " + blockIDString);
				dni.add(new DatanodeInfo(new DatanodeID(new String(host))));
			}		

			// TODO: add generation	
			locatedBlocks.add(
					new LocatedBlock(
							new Block(Long.parseLong(new String(id)), blockSize, 0), 
							dni.toArray(new DatanodeInfo[dni.size()]),
							blockOffset
					)		
			);

			blockOffset += blockSize;
		}

		// TODO: sort locatedBlocks by network-distance from client
		boolean underConst = false;
		return new LocatedBlocks(fileLength, locatedBlocks, underConst);
	}

	@Override
	public ContentSummary getContentSummary(String path) throws IOException {
		System.out.println("using getContentSummary");
		return null;
	}

	@Override
	public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
	throws IOException {
		System.out.println("using getDatanodeReport");
		return null;
	}

	private FileStatus loadFileStatus(Result fileResult) {

		boolean isdir = new String(fileResult.getValue(infoFam, "isDir".getBytes())).equals("Y");
		long modification_time = 0;
		long blocksize = 0;
		int block_replication = 0;
		long length = 0;
		
		if(!isdir) {
			length = Long.parseLong(new String(fileResult.getValue(infoFam, "size".getBytes())));
			block_replication = Integer.parseInt(new String(fileResult.getValue(infoFam, "replication".getBytes())));
			blocksize = Long.parseLong(new String(fileResult.getValue(infoFam,"blocksize".getBytes())));
			// TODO: create time as modification time? maybe updated create_time when complete() is called?
			modification_time = Long.parseLong(new String(fileResult.getValue(infoFam, "create_time".getBytes())));
		}
		
		Path path = new Path(Bytes.toString(fileResult.getRow()));

		return new FileStatus(length, isdir, block_replication,
				blocksize, modification_time, path);
	}

	@Override
	public FileStatus getFileInfo(String src) throws IOException {
		System.out.println("using getFileInfo");
		// TODO: could implement entirely by reading namespace table ..?


		Get fileInfoGet = new Get(src.getBytes());
		Result fileResult = namespaceTable.get(fileInfoGet);

		if(fileResult == null || fileResult.isEmpty())
			throw new FileNotFoundException(src);

		return loadFileStatus(fileResult);
	}

	/**
	 * This method is currently doing a lot of lookups ...
	 */
	@Override
	public FileStatus[] getListing(String src) throws IOException {
		System.out.println("using getListing");

		Get listGet = new Get(src.getBytes());
		listGet.addFamily(childrenFam);
		listGet.addColumn(infoFam, "isDir".getBytes());

		Result listResult = namespaceTable.get(listGet);

		if(listResult == null || listResult.isEmpty())
			throw new IOException("directory not found: " + src);

		if(new String(listResult.getValue(infoFam, "isDir".getBytes())).equals("N"))
			throw new IOException(src + " is not a directory");

		ArrayList<FileStatus> files = new ArrayList<FileStatus>();
		NavigableMap<byte[],byte[]> children = listResult.getFamilyMap(childrenFam);
		for(byte[] child : children.keySet()) {
			Get childGet = new Get(child);
			Result childResult = namespaceTable.get(childGet);
			FileStatus stat = loadFileStatus(childResult);
			files.add(stat);
		}

		return files.toArray(new FileStatus[files.size()]);
	}

	@Override
	public long getPreferredBlockSize(String filename) throws IOException {
		System.out.println("using getPreferredBlockSize");
		return 0;
	}

	@Override
	public long[] getStats() throws IOException {
		System.out.println("using getStats");
		return null;
	}

	@Override
	public void metaSave(String filename) throws IOException {
		System.out.println("using metaSave");

	}

	public static String normalizePath(String src) {
		if (src.length() > 1 && src.endsWith("/")) {
			src = src.substring(0, src.length() - 1);
		}
		return src;
	}

	@Override
	public boolean mkdirs(String src, FsPermission masked) throws IOException {
		System.out.println("using mkdirs");

		if (!DFSUtil.isValidName(src)) {
			throw new IOException("Invalid directory name: " + src);
		}

		// TODO: check permissions

		src = normalizePath(src);

		// TODO: get locks ...
		// verify parent path exists
		byte[] parentPath = getParentPath(src);

		Get parentLookup = new Get(parentPath);
		parentLookup.addColumn(infoFam, "isDir".getBytes());
		parentLookup.addFamily(childrenFam);

		Result result = namespaceTable.get(parentLookup);
		if(result == null || result.isEmpty()) 
			throw new FileNotFoundException(new String(parentPath));


		if(result.getCellValue().equals("N")) 
			throw new IOException("error: parent " + src + " is not a directory");


		if(result.containsColumn(childrenFam, src.getBytes()))
			throw new IOException("directory exists: " + src);

		// edit namespace
		ArrayList<Put> changes = new ArrayList<Put>();
		//String dirName = getDirName(src);

		Put addChild = new Put(parentPath);
		addChild.add(childrenFam, src.getBytes(), "".getBytes());
		changes.add(addChild);

		Put newDir = new Put(src.getBytes());
		newDir.add(infoFam, "isDir".getBytes(), "Y".getBytes());

		changes.add(newDir);

		namespaceTable.put(changes);
		return true;
	}

	private static String getDirName(String src) {
		if(src.equals("/"))
			return "/";
		String[] components = src.split("/");
		return components[components.length-1];
	}

	private static byte[] getParentPath(String src) {
		if(src.equals("/"))
			return "/".getBytes();

		String[] components = src.split(Path.SEPARATOR);

		StringBuilder sb = new StringBuilder();
		for(int i=0; i < components.length - 1; i++)
			sb.append(components[i] + "/");

		if(sb.length() > 1)
			sb.deleteCharAt(sb.length()-1);
		
		return sb.toString().getBytes();
	}

	@Override
	public void refreshNodes() throws IOException {
		System.out.println("using refreshNodes");

	}

	@Override
	public boolean rename(String src, String dst) throws IOException {
		System.out.println("using rename");
		return false;
	}

	@Override
	public void renewLease(String clientName) throws IOException {
		System.out.println("using renewLease");

	}

	@Override
	public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
		System.out.println("using reportBadBlocks");

	}

	@Override
	public void saveNamespace() throws IOException {
		System.out.println("using saveNamespace");

	}

	@Override
	public void setOwner(String src, String username, String groupname)
	throws IOException {
		System.out.println("using setOwner");

	}

	@Override
	public void setPermission(String src, FsPermission permission)
	throws IOException {
		System.out.println("using setPermission");

	}

	@Override
	public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
	throws IOException {
		System.out.println("using setQuota");

	}

	@Override
	public boolean setReplication(String src, short replication)
	throws IOException {
		System.out.println("using setReplication");
		return false;
	}

	@Override
	public boolean setSafeMode(SafeModeAction action) throws IOException {
		System.out.println("using setSafeMode");
		return false;
	}

	@Override
	public void setTimes(String src, long mtime, long atime) throws IOException {
		System.out.println("using setTimes");

	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
	throws IOException {
		System.out.println("using getProtocolVersion");
		return 0;
	}



	/** ------------ Data Node Protocol Methods -----------
	 * 
	 */

	@Override
	public void blockReceived(DatanodeRegistration registration,
			Block[] blocks, String[] delHints) throws IOException {
		System.out.println("using blockReceived");

		// for each block we should have recorded its existence already
		// we should also know about the datanode

		// update blocks table
		ArrayList<Put> blockUpdates = new ArrayList<Put>();
		for(Block b : blocks) {
			Put blockData = new Put(Long.toString(b.getBlockId()).getBytes());
			blockData.add(infoFam, "size".getBytes(), Bytes.toBytes(Long.toString(b.getNumBytes())));
			blockUpdates.add(blockData);
		}

		blocksTable.put(blockUpdates);

		// update total file space ?
	}

	@Override
	public DatanodeCommand blockReport(DatanodeRegistration registration,
			long[] blocks) throws IOException {
		System.out.println("using blockReport");
		// data node is coming online
		// update blocks table and datanodes table
		recordHostBlocks(registration.name, blocks);
		for(long block : blocks)
			System.out.println(block);

		return null;
	}

	@Override
	public void commitBlockSynchronization(Block block,
			long newgenerationstamp, long newlength, boolean closeFile,
			boolean deleteblock, DatanodeID[] newtargets) throws IOException {
		System.out.println("using commitBlockSynchronization");

	}

	@Override
	public void errorReport(DatanodeRegistration registration, int errorCode,
			String msg) throws IOException {
		System.out.println("using errorReport");

	}

	@Override
	public long nextGenerationStamp(Block block) throws IOException {
		System.out.println("using nextGenerationStamp");
		return 0;
	}

	@Override
	public DatanodeRegistration register(DatanodeRegistration registration)
	throws IOException {
		System.out.println("using register");
		// record this datanode's info
		Put reg = new Put(registration.name.getBytes());
		reg.add(infoFam, "storageID".getBytes(), registration.storageID.getBytes());

		datanodesTable.put(reg);

		// clients get this info in a list of targets from addBlock()

		return registration;
	}

	@Override
	public UpgradeCommand processUpgradeCommand(UpgradeCommand comm)
	throws IOException {
		System.out.println("using processUpgradeCommand");
		return null;
	}

	@Override
	public DatanodeCommand[] sendHeartbeat(DatanodeRegistration registration,
			long capacity, long dfsUsed, long remaining, int xmitsInProgress,
			int xceiverCount) throws IOException {
		//System.out.println("using sendHeartbeat");

		// update datanodes table with info
		// skip this if none of the numbers have changed
		if(capacity != lastCapacity || 
				dfsUsed != lastDfsUsed ||
				remaining != lastRemaining) {
			Put put = new Put(registration.name.getBytes());
			put.add(infoFam, "capacity".getBytes(), Long.toString(capacity).getBytes());
			put.add(infoFam, "used".getBytes(), Long.toString(dfsUsed).getBytes());
			put.add(infoFam, "remaining".getBytes(), Long.toString(remaining).getBytes());

			lastCapacity = capacity;
			lastDfsUsed = dfsUsed;
			lastRemaining = remaining;

			datanodesTable.put(put);
		}		

		// return a list of commands for the data node
		// ping the health server and return any commands received
		return null; //healthServer.heartbeat(registration);
	}

	@Override
	public NamespaceInfo versionRequest() throws IOException {
		System.out.println("using versionRequest");
		// TODO: find out how to get namespace id
		// could store this in the info of the / entry
		NamespaceInfo nsi = new NamespaceInfo(384837986, 0, 0);
		//throw new RuntimeException();
		return nsi;
	}

	public void stop() {
		// TODO Auto-generated method stub	
		try {
			namespaceTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			blocksTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			datanodesTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int computeDatanodeWork() {
		System.out.println("using computeDatanodeWork");
		// TODO: how is this number used by the caller?
		return 0;
	}

	public static void initializeCluster() throws IOException {
		HBaseConfiguration config = new HBaseConfiguration();

		HBaseAdmin admin = new HBaseAdmin(config);

		HTableDescriptor[] tableList = admin.listTables();
		HashSet<String> tableSet = new HashSet<String>();
		for(HTableDescriptor table : tableList)
			tableSet.add(table.getNameAsString());

		if(!tableSet.contains("namespace")) {
			HTableDescriptor namespaceTable = new HTableDescriptor("namespace");
			namespaceTable.addFamily(new HColumnDescriptor("info"));
			namespaceTable.addFamily(new HColumnDescriptor("blocks"));
			namespaceTable.addFamily(new HColumnDescriptor("children"));
			admin.createTable(namespaceTable);
		}

		if(!tableSet.contains("blocks")) {
			HTableDescriptor blocksTable = new HTableDescriptor("blocks");
			blocksTable.addFamily(new HColumnDescriptor("datanodes"));
			blocksTable.addFamily(new HColumnDescriptor("info"));
			admin.createTable(blocksTable);
		}

		if(!tableSet.contains("datanodes")) {
			HTableDescriptor datanodesTable = new HTableDescriptor("datanodes");
			datanodesTable.addFamily(new HColumnDescriptor("blocks"));
			datanodesTable.addFamily(new HColumnDescriptor("info"));
			admin.createTable(datanodesTable);
		}


		HTable namespace = new HTable("namespace");
		Get rootGet = new Get("/".getBytes());
		Result r = namespace.get(rootGet);

		if(r == null || r.isEmpty()) {
			System.out.println("creating root directory");
			Put rootPut = new Put("/".getBytes());
			rootPut.add("info".getBytes(), "isDir".getBytes(), "Y".getBytes());
			rootPut.add("info".getBytes(), "create_time".getBytes(), Long.toString(System.currentTimeMillis()).getBytes());
			namespace.put(rootPut);
		}

		System.out.println("HBase tables initialized");
	}

	public static void main(String[] args) throws IOException {

		initializeCluster();

	}

}
