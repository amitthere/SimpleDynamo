package edu.buffalo.cse.cse486586.simpledynamo;

import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.NavigableMap;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String[] PORTS = {"11108","11112","11116","11120","11124"};
	static final String KEY_FIELD = "key";
	static final String VALUE_FIELD = "value";
	public DynamoDB dynamoDB;
	public SQLiteDatabase db;
	Uri uri;
	static final int SERVER_PORT = 10000;
	String myPort;
	String myNodeId;
	String prevNodeId;
	String nextNodeId;
	// reference - https://docs.oracle.com/javase/8/docs/api/java/util/NavigableMap.html
	volatile NavigableMap<String, String> hashedNodeIdToPort = new TreeMap<String, String>();
	public enum msgType {
		INSERT,QUERY,DELETE
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public boolean onCreate() {
		// TODO setup ring

		// setup database
		this.dynamoDB = new DynamoDB(this.getContext());
		this.db = dynamoDB.getWritableDatabase();

		Log.i(TAG,"Executing ContentProvider OnCreate()");
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		myNodeId = portToNodeID(myPort);
		prevNodeId = myNodeId;
		nextNodeId = myNodeId;

		//create server socket, which listens on this node
		try{
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (Exception e) {
			Log.e(TAG, "Unable to create a ServerSocket : " + e.getMessage());
			return false;
		}

		uri = buildUri("content","edu.buffalo.cse.cse486586.simpledynamo.provider");

		setupRing();

		Log.i(TAG,"port:nodeID " + myPort + ":" + myNodeId);
		Log.i(TAG,"prev: " + prevNodeId + " next:" + nextNodeId);
		Log.i(TAG,"Ring: " + hashedNodeIdToPort.toString());
		return false;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO handle *
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO handle * and @
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets){
			return null;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			return null;
		}

	}

	synchronized private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	synchronized private String genHashWithTry(String input){
		String hash="initializer";
		try {
			hash = genHash(input);
		} catch(NoSuchAlgorithmException nsae){
			Log.e(TAG,"NoSuchAlgorithmException "+ nsae.getMessage());
		} catch (Exception e){
			Log.e(TAG,"Random Exception " + e.getMessage());
		}
		return hash;
	}


	public void setupRing(){

		String myNodeIdHash = genHashWithTry(myNodeId);

		// add NodeID hash as key and port as value in the map
		for(String port: PORTS) {
			hashedNodeIdToPort.put(genHashWithTry(portToNodeID(port)), port);
		}

		if(hashedNodeIdToPort.firstKey().equals(myNodeIdHash)) {			// first node in the ring with smallest hash

			prevNodeId = portToNodeID(hashedNodeIdToPort.get(hashedNodeIdToPort.lastKey()));
			nextNodeId = portToNodeID(hashedNodeIdToPort.get(hashedNodeIdToPort.higherKey(myNodeIdHash)));

		} else if (hashedNodeIdToPort.lastKey().equals(myNodeIdHash)) {	// last node in the ring with largest hash

			prevNodeId = portToNodeID(hashedNodeIdToPort.get(hashedNodeIdToPort.lowerKey(myNodeIdHash)));
			nextNodeId = portToNodeID(hashedNodeIdToPort.get(hashedNodeIdToPort.firstKey()));

		} else {															// otherwise

			prevNodeId = portToNodeID(hashedNodeIdToPort.get(hashedNodeIdToPort.lowerKey(myNodeIdHash)));
			nextNodeId = portToNodeID(hashedNodeIdToPort.get(hashedNodeIdToPort.higherKey(myNodeIdHash)));
		}
	}

	synchronized public String nodeIdToPort(String nodeID){
		int intPort = Integer.parseInt(nodeID)*2;
		return String.valueOf(intPort);
	}

	synchronized public String portToNodeID(String port){
		int nodeID = Integer.parseInt(port)/2;
		return String.valueOf(nodeID);
	}

	// reference - https://developer.android.com/training/data-storage/sqlite#java
	public class DynamoDB extends SQLiteOpenHelper {

		public static final int DATABASE_VERSION = 1;
		public static final String DATABASE_NAME = "DynamoDB.db";
		public static final String DATABASE_TABLE = "Dynamo_Table";

		public DynamoDB(Context context) {
			super(context, DATABASE_NAME, null, DATABASE_VERSION);
		}

		public void onCreate(SQLiteDatabase db) {
			db.execSQL(
					"CREATE TABLE " + DATABASE_TABLE + "(" +
							"key TEXT, " +
							"value TEXT, " +
							"PRIMARY KEY (key)" +
					");"
			);
		}

		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

		}

		public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {

		}
	}
}
