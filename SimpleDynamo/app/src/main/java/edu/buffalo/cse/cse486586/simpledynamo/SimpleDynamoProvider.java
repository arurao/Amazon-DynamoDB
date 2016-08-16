package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.concurrent.ExecutionException;

public class SimpleDynamoProvider extends ContentProvider {


	private MessengerOpenHelper db;
	static final String TAG = SimpleDynamoActivity.class.getSimpleName();

	static final String REMOTE_PORT0 = "11108";
	private ServerSocket serverSocket;
	static final int SERVER_PORT = 10000;
	private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	static String to_hash = "";
	static String myPort = "";
	static ArrayList<String> aliveList = new ArrayList<String>();
	static ArrayList<Integer> to_be_inserted_list = new ArrayList<Integer>();
	static ArrayList<Integer> temp_list = new ArrayList<Integer>();
	static String predecessor = "";
	static String successor = "";
	static String query_result = "";
	static String total_result = "";
	static String result_for_query = "";
	static int failed_node=-1;

	boolean flag = false;
	boolean request = false;
	boolean insertion = false;
	boolean recover = true;

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}


	public static Comparator<String> priorityComparator = new Comparator<String>() {
		public int compare(String m1,String m2)
		{
			//Log.e(TAG,"In Comparator "+m1+" "+m2);
			try {
				return genHash(m1).compareTo(genHash(m2));


			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			return 0;
		}
	};




	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// where clause, column names
		SQLiteDatabase database = db.getWritableDatabase();
		String[] sArgs={selection};


		Cursor cursor = database.query("simple_dynamo",null,"key='"+selection+"'",selectionArgs,null,null,null);


		if(cursor.getCount()<1) {
			Log.e(TAG,"Entry not present in database of this AVD");
			return 0;

		} else {
			database.delete("simple_dynamo","key = ?",sArgs);


		}

		Log.e("Deletion", "From content provider" + selectionArgs.toString());
		return 1;
	}

	@Override
	public synchronized String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}


	// MAY 4 4 AM ADDED SYNCH
	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         *
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */


		// NOW WHILE INSERTING WE SHUD HASH THE KEY AND COMPARE WITH HASHED VALUES OF NODES IN RING.
		// AND ACCORDINGLY SEND IT TO ITS SUCC OR KEEP IT TO ITSELF

		try {


			Log.e(TAG,"IN INSERT FOR : "+values.toString());


			// RETURNING THE CORRECT LOCATION OF THE HASHED KEY
			int position = check(values.getAsString("key"));
			Log.e(TAG,"KEY "+values.getAsString("key")+" SHOULD BE AT POSITION "+position);


			int my_position = -1;

			if(Integer.parseInt(myPort) == 11124)
				my_position = 0;
			if(Integer.parseInt(myPort) == 11112)
				my_position = 1;
			if(Integer.parseInt(myPort) == 11108)
				my_position = 2;
			if(Integer.parseInt(myPort) == 11116)
				my_position = 3;
			if(Integer.parseInt(myPort) == 11120)
				my_position = 4;




/*
			for(int h=0;h<aliveList.size();h++)
			{
				if( convert_back(aliveList.get(h)).equals(myPort))
				{
					my_position = h;
				}
			}
*/
			Log.e(TAG,"MY POSITION "+my_position);

			to_be_inserted_list.clear();
			// ADDING THAT POSITION AND ITS NEXT 2 SUCCESSORS INTO TO_BE_INSERTED_LIST

			to_be_inserted_list.add(position);


			if(position == aliveList.size()-1) // if last node in list
			{
				to_be_inserted_list.add(0);
				to_be_inserted_list.add(1);
			}
			else if(position == aliveList.size()-2) // if second last node
			{
				to_be_inserted_list.add(aliveList.size()-1);
				to_be_inserted_list.add(0);
			}
			else
			{
				to_be_inserted_list.add(position + 1);
				to_be_inserted_list.add(position + 2);
			}

			String insertion_list = "";
			Log.e(TAG,"TO_BE_INSERTED_LIST SIZE: "+to_be_inserted_list.size());

			for(int y=0; y<to_be_inserted_list.size(); y++)
			{
				Log.e(TAG, " " + to_be_inserted_list.get(y));
				insertion_list = insertion_list + ";" + to_be_inserted_list.get(y);
			}


			for(int s=0;s<to_be_inserted_list.size();s++)
			{

				if (my_position == to_be_inserted_list.get(s)) // BOTH INTEGERS COMPARISON
				{
					//ARUNDHATI

					//CHANGED 5 MAY 1:30AM
					to_be_inserted_list.remove(s);

					Log.e(TAG, "FIRST INSERTING INTO MYSELF");
					SQLiteDatabase database = db.getReadableDatabase();


					ContentValues cv_first = new ContentValues();
					cv_first.put("key", values.getAsString("key"));
					cv_first.put("value", values.getAsString("value"));

					String[] selectionArgs = {values.getAsString("key")};

					Cursor cursor_first = database.query("simple_dynamo", null, "key = ?", selectionArgs, null, null, null);

					if (cursor_first.getCount() < 1) {
						database.insert("simple_dynamo", null, cv_first);

					} else {
						database.update("simple_dynamo", cv_first, "key = ?", selectionArgs); // if already existed values
					}

					Log.e("inserted IN SELF", "and in content provider" + cv_first.toString());


					//insertion = true;
				}

			}

			//NOW ACTUALLY INSERTING IT AT THOSE POSITIONS

			// 4 May 11 pm Changed from execute() to executeonExecutor

			Log.e(TAG,"BEFORE CALLING CLIENTASK AFTER/NOT AFTER INSERTING INTO SELF");

			/*
for(int g=0;g<to_be_inserted_list.size();g++)
{

	int x = to_be_inserted_list.get(g);
int to_send_port = -1;

	if ( x == 0)
		to_send_port = 11124;
	if ( x == 1)
		to_send_port = 11112;
	if ( x == 2)
		to_send_port = 11108;
	if ( x == 3)
		to_send_port = 11116;
	if ( x == 4)
		to_send_port = 11120;

	//------------------------------------










	///------------------------
	Log.e(TAG,"SENDING INSERT_REQUEST FOR "+values.getAsString("key")+"TO "+to_send_port+" FROM INSERT() DIRECTLY!");

	Socket socket_insert_replica = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), to_send_port );
	PrintWriter out_replica_insert = new PrintWriter(socket_insert_replica.getOutputStream(), true);
	out_replica_insert.println("INSERT_REQUEST;" + values.toString());

	BufferedReader iMsgStream = new BufferedReader(new InputStreamReader(socket_insert_replica.getInputStream()));
	String message_insert_replica = iMsgStream.readLine();


	if(message_insert_replica == null)
	{
		Log.e(TAG,"STUPID NODE HAS CRASHED; SEND IT TO ITS SUCCESSORS : "+get_Successor(to_send_port)+ " & "+get_Successor(get_Successor(to_send_port)));

		Socket socket_insert_replica_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), get_Successor(to_send_port) );
		PrintWriter out_replica_insert_1 = new PrintWriter(socket_insert_replica_1.getOutputStream(), true);
		out_replica_insert_1.println("INSERT_REQUEST;" + values.toString());
		socket_insert_replica_1.close();

		Socket socket_insert_replica_2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), get_Successor(get_Successor(to_send_port)) );
		PrintWriter out_replica_insert_2 = new PrintWriter(socket_insert_replica_2.getOutputStream(), true);
		out_replica_insert_2.println("INSERT_REQUEST;" + values.toString());
		socket_insert_replica_2.close();



	}


	socket_insert_replica.close();
*/
			new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"INSERT"+insertion_list+";"+values.toString(), myPort);
//}

		}
		catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}

		return uri;
	}


	@Override
	public  boolean onCreate() {
		//-------------------------------------------


		Context context = getContext();
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		db = new MessengerOpenHelper(getContext(), null, null, 1);
		//ASK ANY ONE NODE IF MYPORT == FAILED NODE VALUE (I MYSELF WONT KNOW)

		// 4 May 11 pm Changed from execute() to executeonExecutor
		new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"RECOVERED",myPort);

		try {


			initialise_List();

			decide_PnS();

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT,10000);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		}

		catch (Exception e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}

		return true;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
									 String sortOrder) {
       /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */

/*


		while(recover !=true)
		{
			try {
				Thread.sleep(100); // 100
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(recover == true)
				break;
		}

		//recover=false;


*/

		SQLiteDatabase database = db.getReadableDatabase();
		//String [] sArgs = new String[0];
		Cursor cursor = null;


		Log.e(TAG, "IN QUERY :: FOR " + selection);

		if(selection.equals("@")) {
			Log.e(TAG, "EQUALS @");
			//ALL PAIRS STORED IN LOCAL PARTITION

			//cursor = database.query("simple_dht", null, "key='" + selection + "'", selectionArgs, null, null, null);
			cursor = database.rawQuery("SELECT * from simple_dynamo",null);
			Log.e(TAG,"Count of cursor @ = "+cursor.getCount());
			return cursor;
		}


		else if(selection.equals("*") ) {
			Log.e(TAG, "EQUALS *");
			//RETURN ALL PAIRS STORED IN ENTIRE RING

			String q = "*";
			Log.e(TAG, "Getting DBs from all others");

			Cursor dbs = request_for_db();
			return dbs;

		}

		else {

			Log.e(TAG, "QUERYING FOR SINGLE KEY : "+selection); // ISSUE HERE?
			//Log.e(TAG,"SENDING TO CLIENTTASK WITH : "+selection + "%%" + myPort);
			flag=false;


			int position = check(selection);
			Log.e(TAG,"KEY SHOULD BE AT POSITION "+position);


			aliveList.clear();


			aliveList.add("5562");
			aliveList.add("5556");
			aliveList.add("5554");
			aliveList.add("5558");
			aliveList.add("5560");


			int my_position = -1;

			if(Integer.parseInt(myPort) == 11124)
				my_position = 0;
			if(Integer.parseInt(myPort) == 11112)
				my_position = 1;
			if(Integer.parseInt(myPort) == 11108)
				my_position = 2;
			if(Integer.parseInt(myPort) == 11116)
				my_position = 3;
			if(Integer.parseInt(myPort) == 11120)
				my_position = 4;




/*
			for(int h=0;h<aliveList.size();h++)
			{
				if( convert_back(aliveList.get(h)).equals(myPort))
				{
					my_position = h;
				}
			}
*/
			Log.e(TAG,"MY POSITION "+my_position);

			temp_list.clear();
			// ADDING THAT POSITION AND ITS NEXT 2 SUCCESSORS INTO TO_BE_INSERTED_LIST

			temp_list.add(position);


			if(position == aliveList.size()-1) // if last node in list
			{
				temp_list.add(0);
				temp_list.add(1);
			}
			else if(position == aliveList.size()-2) // if second last node
			{
				temp_list.add(aliveList.size()-1);
				temp_list.add(0);
			}
			else
			{
				temp_list.add(position + 1);
				temp_list.add(position + 2);
			}

			//String temp_list = "";
			Log.e(TAG,"TEMP_LIST SIZE: "+temp_list.size());

			for(int y=0; y<temp_list.size(); y++)
			{
				Log.e(TAG, " " + temp_list.get(y));
				//insertion_list = insertion_list + ";" + to_be_inserted_list.get(y);
			}


			//ISSUE HERE : ALIVELIST IS EMPTY HERE! HOW ?


			String expected_location = aliveList.get(position);
			String expected_port = convert_back(expected_location);
			Log.e(TAG,"EXPECTED PORT "+expected_port);



			int my_location_in_aliveList = -1;

			for(int x=0;x<aliveList.size();x++)
			{
				if(convert_back(aliveList.get(x)).equals(myPort))
					my_location_in_aliveList = x;
			}

			Log.e(TAG,"MY LOCATION IN ALIVELIST = "+my_location_in_aliveList);


			if(myPort.equals(expected_port) || temp_list.get(1) == my_location_in_aliveList || temp_list.get(2) == my_location_in_aliveList )
			{
				Log.e(TAG,"MYPORT AND EXPECTED PORT ARE SAME; ONE AMONGST TO BE INSERTED LIST");
//				Cursor cursor_1 = database.query("simple_dynamo", null, "key='" + selection + "'", selectionArgs, null, null, null);

				//	QUERY IS FASTER THAN INSERT

				int g=0;
				while(g<1000)
					g++;

				Cursor cursor_self = database.query("simple_dynamo", null, "key='" + selection + "'", null, null, null, null);

				cursor_self.moveToFirst();


				//COMMENTING 4MAY 3 AM

				/*while(cursor_self.getCount()<1)
				{
					Log.e(TAG,"WAITING FOR INSERTION == TRUE");
					//MAKE IT WAIT TIL INSERTION HAPPENS
					if(insertion == true)
					{
						Log.e(TAG,"BREAK");
						break;
					}

				}*/

				insertion = false;

				Cursor cursor_self1 = database.query("simple_dynamo", null, "key='" + selection + "'", null, null, null, null);

				cursor_self1.moveToFirst();
				Log.v("query", selection+" count = "+cursor_self1.getCount());

				//cursor_self.close();
				return cursor_self;
			}

			else
			{
				// NOT IN MY DB QUERY OTHERS DB
				Log.e(TAG,"MYPORT AND EXPECTED PORT ARE NOT SAME!!!");

				Cursor cursor_query=null;
				try {


					//cursor_query = new ClientTask_Query().execute("QUERY_REQUEST;"+selection+";"+expected_port);

					//  4 May 11Pm changed from Serial to Thread_Pool
					cursor_query = new ClientTask_Query().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "QUERY_REQUEST;"+selection+";"+expected_port).get();

					//cursor_query = new ClientTask_Query().execute("QUERY_REQUEST;"+selection+";"+expected_port).get();


				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}


				//new ClientTask().execute("QUERY_REQUEST;"+selection+";"+expected_port);

				flag=false;

				//Log.e(TAG, "FLAG " + flag);

/*				while (flag != true) {
					try {
						Thread.sleep(100); // 1000
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					Log.e(TAG,"Before flag becomes true for "+selection+" from port "+expected_port);
					if (flag == true)
						break;
				}
*/
				flag = false;

				flag = false;
				return cursor_query;

			}


		}


	}

	//MAY 4 4 AM ADDED SYNCH
	@Override
	public synchronized int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	public static String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}





	private class ServerTask extends AsyncTask<ServerSocket, String, Void>
	{

		// receives messages
		private ObjectInputStream ipStream;
		private ObjectOutputStream opStream;
		private Socket socket;
		private String message;
		@Override
		protected  Void doInBackground(ServerSocket... sockets)
		{
			serverSocket = sockets[0];
			ObjectInputStream input;
			Socket socket = null;
			String result="";

			Log.e(TAG,"IN SERVERTASK");


			//Keep listening to messages from other clients

			try {
				while (true) {
					// Accept Connection and Initialize Input Stream
					socket = serverSocket.accept();
					BufferedReader iMsgStream = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String message = iMsgStream.readLine();

					Log.e(TAG, "IN SERVERTASK MESSAGE " + message);



					if(message.contains("REPLICA_REQUEST"))
					{
						Log.e(TAG,"REPLICA_REQUEST MESSAGE IN SERVERTASK "+myPort);

						int actual_position = -1;

						//FINDING POSITION OF NODE IN LIST
						for(int d=0;d<aliveList.size();d++)
						{
							if(convert_back(aliveList.get(d)).equals(myPort))
							{
								actual_position = d;
							}

						}

						//FINDING POSITION PREDECESSOR OF NODE IN LIST

						int pred_of_myPort = get_Predecessor(Integer.parseInt(myPort));

						Log.e(TAG,"pred_of_myPort = "+pred_of_myPort);


						int actual_position_of_pred_of_myPort = -1;

						for(int q=0;q<aliveList.size();q++)
						{
							if(convert_back(aliveList.get(q)).equals(Integer.toString(pred_of_myPort)))
							{
								actual_position_of_pred_of_myPort = q;
							}

						}

						Log.e(TAG,"POSITION OF MYPORT "+actual_position);
						Log.e(TAG,"POSITION OF PRED OF MYPORT "+actual_position_of_pred_of_myPort);

						SQLiteDatabase database = db.getWritableDatabase();
						Cursor c1 = database.rawQuery("SELECT * from simple_dynamo",null);

						String keydump = "";
						String valuedump = "";

						String keydump_of_pred="";
						String valuedump_of_pred="";

						int keyIndex = c1.getColumnIndex("key");
						int valueIndex = c1.getColumnIndex("value");

						c1.moveToFirst();

						String returnKey = c1.getString(keyIndex);
						String returnValue = c1.getString(valueIndex);

						c1.moveToFirst();

						while(!c1.isAfterLast())
						{
							returnKey = c1.getString(keyIndex);
							returnValue = c1.getString(valueIndex);

							//KEY SHOULD BE AT POSITION
							int returned_position = check(returnKey);
							Log.e(TAG,"KEY SHOULD BE AT POSITION "+returned_position);

							if(actual_position == returned_position)
							{
								//MEANS THIS KEY BELONGS TO NODE; SEND BACK
								keydump = keydump + ":" + returnKey;
								valuedump = valuedump + ":" + returnValue;

							}

							if(actual_position_of_pred_of_myPort == returned_position)
							{
								//MEANS THIS KEY BELONGS TO NODE; SEND BACK

								// NO OF KEYS > NO OF VALUES!!! ISSUE HERE : TERRIBLY WRONG!
								keydump_of_pred = keydump_of_pred + ":" + returnKey;
								valuedump_of_pred = valuedump_of_pred + ":" + returnValue;

							}



							c1.moveToNext();

						}


						String answer = keydump + "#" +valuedump +"&" + keydump_of_pred +"#" +valuedump_of_pred;

						Log.e(TAG,"ANSWER 1 -->"+answer);

						PrintWriter pout = new PrintWriter(socket.getOutputStream(), true);
						pout.println(answer);

					}

					if(message.contains("FAILED NODE"))
					{
						Log.e(TAG,"FAILED NODE BROADCAST MESSAGE IN SERVERTASK");

						String[] failed_node_parts = message.split(":");

						failed_node= Integer.parseInt(failed_node_parts[1].toString());

						Log.e(TAG,"SET FAILED NODE FROM BROADCASTED MESSAGE "+failed_node);

					}

					if(message.equals("aaa"))
					{
						Log.e(TAG,"PING REQUEST SERVERTASK SENDING ACK");


						PrintWriter pout = new PrintWriter(socket.getOutputStream(), true);
						pout.println("ACK");

					}

					if(message.contains("RECOVERED"))
					{

						int actual_position = -1;

						Log.e(TAG,"RECOVERED MESSAGE IN SERVERTASK");

						String[] msg_parts = message.split(":");

						if(Integer.parseInt(msg_parts[1]) == failed_node)
						{
							Log.e(TAG,"FOUND RECOVERED NODE "+failed_node);
							Log.e(TAG,"DO RECOVERY OPERATIONS HERE");



							//FINDING POSITION OF FAILED NODE IN LIST
							for(int d=0;d<aliveList.size();d++)
							{
								if(convert_back(aliveList.get(d)).equals(Integer.toString(failed_node)))
								{
									actual_position = d;
								}

							}

							Log.e(TAG,"ACTUAL POSITION OF FAILED NODE IN LIST = "+actual_position);


							// SUCCESSOR HAS FOUND RECOVERED NODE. RECOVERED NODE SHUD ASK IT WHAT ALL KEY-VALUE PAIRS IT MISSED.

//---------------------
							SQLiteDatabase database = db.getWritableDatabase();
							Cursor c1 = database.rawQuery("SELECT * from simple_dynamo",null);

							String keydump = "";
							String valuedump = "";

							int keyIndex = c1.getColumnIndex("key");
							int valueIndex = c1.getColumnIndex("value");

							//Log.e(TAG, "Keyindex " + keyIndex + "  " + valueIndex);

							c1.moveToFirst();

							String returnKey = c1.getString(keyIndex);
							String returnValue = c1.getString(valueIndex);

							c1.moveToFirst();

							while(!c1.isAfterLast())
							{
								returnKey = c1.getString(keyIndex);
								returnValue = c1.getString(valueIndex);

								//KEY SHOULD BE AT POSITION
								int returned_position = check(returnKey);
								Log.e(TAG,"KEY SHOULD BE AT POSITION "+returned_position);

								if(actual_position == returned_position)
								{
									//MEANS THIS KEY BELONGS TO FAILED NODE; INSERT INTO FAILED NODE
									keydump = keydump + ":" + returnKey;
									valuedump = valuedump + ":" + returnValue;

								}

								c1.moveToNext();

							}


							String answer = keydump + "#" +valuedump;

							Log.e(TAG,"ANSWER 2 -->"+answer);

							PrintWriter pout = new PrintWriter(socket.getOutputStream(), true);
							pout.println(answer);


//------------------
						}
						else
						{
							PrintWriter pout = new PrintWriter(socket.getOutputStream(), true);
							pout.println("FILLER");

						}


					}


					if(message.contains("RECOVERED_PREDECESSOR"))
					{

						int actual_position = -1;

						Log.e(TAG,"RECOVERED PREDECESSOR MESSAGE IN SERVERTASK");

						String[] msg_parts = message.split(":");

						if(Integer.parseInt(msg_parts[1]) == failed_node)
						{
							Log.e(TAG,"FOUND RECOVERED NODE "+failed_node);
							Log.e(TAG,"DO RECOVERY OPERATIONS HERE");



							//FINDING POSITION OF FAILED NODE IN LIST
							for(int d=0;d<aliveList.size();d++)
							{
								if(convert_back(aliveList.get(d)).equals(Integer.toString(failed_node)))
								{
									actual_position = d;
								}

							}

							Log.e(TAG,"ACTUAL POSITION OF FAILED NODE IN LIST = "+actual_position);


							// SUCCESSOR HAS FOUND RECOVERED NODE. RECOVERED NODE SHUD ASK IT WHAT ALL KEY-VALUE PAIRS IT MISSED.

//---------------------
							SQLiteDatabase database = db.getWritableDatabase();
							Cursor c1 = database.rawQuery("SELECT * from simple_dynamo",null);

							String keydump = "";
							String valuedump = "";

							int keyIndex = c1.getColumnIndex("key");
							int valueIndex = c1.getColumnIndex("value");

							//Log.e(TAG, "Keyindex " + keyIndex + "  " + valueIndex);

							c1.moveToFirst();

							String returnKey = c1.getString(keyIndex);
							String returnValue = c1.getString(valueIndex);

							c1.moveToFirst();

							while(!c1.isAfterLast())
							{
								returnKey = c1.getString(keyIndex);
								returnValue = c1.getString(valueIndex);

								//KEY SHOULD BE AT POSITION
								int returned_position = check(returnKey);
								Log.e(TAG,"KEY SHOULD BE AT POSITION "+returned_position);

								if(actual_position == returned_position)
								{
									//MEANS THIS KEY BELONGS TO FAILED NODE; INSERT INTO FAILED NODE
									keydump = keydump + ":" + returnKey;
									valuedump = valuedump + ":" + returnValue;

								}

								c1.moveToNext();

							}


							String answer = keydump + "#" +valuedump;

							Log.e(TAG,"ANSWER 3-->"+answer);

							PrintWriter pout = new PrintWriter(socket.getOutputStream(), true);
							pout.println(answer);


//------------------
						}
						else
						{
							PrintWriter pout = new PrintWriter(socket.getOutputStream(), true);
							pout.println("FILLER");

						}


					}


					if(message.contains("INSERT_REQUEST"))
					{
						Log.e(TAG,"INSERT_REQUEST MESSAGE RECEIVED");

						//BLOCK THIS INSERTION TILL RECOVERY HAPPENS FULLY.

						/*Log.e(TAG,"BLOCK THIS INSERTION TILL RECOVERY HAPPENS FULLY");

						recover = false;

						Log.e(TAG,"RECOVER = FALSE");

						while(recover!=true)
						{

							Log.e(TAG,"RECOVER = FALSE");
							try {
								Thread.sleep(50); // 100
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							if(recover == true) {
								Log.e(TAG,"RECOVER = TRUE NOW");
								break;
							}
						}

						recover = false;

						*/
						String[] split = message.split(";");
						//Log.e(TAG,"SPLIT[1] "+split[1]);

						String[] data=split[1].split(" ");

						String value = data[0].replace("value=","");
						Log.e(TAG,"VALUE "+value);


						String key = data[1].replace("key=","");
						Log.e(TAG,"KEY "+key);


						ContentValues cv = new ContentValues();
						cv.put("key", key);
						cv.put("value", value);


						SQLiteDatabase database = db.getWritableDatabase();
						String[] selectionArgs = {key};

						Cursor cursor = database.query("simple_dynamo", null, "key = ?", selectionArgs, null, null, null);

						if (cursor.getCount() < 1) {
							database.insert("simple_dynamo", null, cv);

						} else {
							database.update("simple_dynamo", cv, "key = ?", selectionArgs); // if already existed values
						}

						Log.e("inserted", "and in content provider" + cv.toString());

						PrintWriter pout1234 = new PrintWriter(socket.getOutputStream(), true);
						pout1234.println("INSERT_REQUEST FULFILLED!");



						insertion = true;


					}

					if(message.contains("QUERY_REQUEST"))
					{

						String parts[] = message.split(";");
						Log.e(TAG,"QUERY REQUEST MESSAGE IN SERVERTASK "+parts[1]);

						// SEARCH FOR THIS KEY PARTS[1]
						//QUERY THIS PERSONS DB FOR THIS KEY

						SQLiteDatabase database = db.getWritableDatabase();
						Cursor c1 = database.query("simple_dynamo", null, "key='" + parts[1] + "'", null, null, null, null);

						//Cursor c1 = database.rawQuery("SELECT * from simple_dynamo",null);

						String keydump="";
						String valuedump="";

						int keyIndex = c1.getColumnIndex("key");
						int valueIndex = c1.getColumnIndex("value");

						Log.e(TAG, "Keyindex " + keyIndex + "  " + valueIndex);

						c1.moveToFirst();

						while(c1.getCount()<1)
						{
							//REQUERY AGAIN. INSERT MIGHT NOT HAVE COMPLETED
							Log.e(TAG,"REQUERYING IN SERVERTASK !!!!!");
							c1 = database.query("simple_dynamo", null, "key='" + parts[1] + "'", null, null, null, null);

							c1.moveToFirst();

							if(c1.getCount() < 1)
							{
								//IF THIS STILL DOES NOT WORK, GET FROM SUCCESSOR HERE!
								Log.e(TAG,"SENDING TO SUCCESSOR "+get_Successor(Integer.parseInt(myPort)) +" AS I(" + myPort + " ) CAN NOT GET IT MYSELF!");
								Socket socket_get_from_successor = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), get_Successor(Integer.parseInt(myPort)));
								PrintWriter out_get_from_successor = new PrintWriter(socket_get_from_successor.getOutputStream(), true);
								out_get_from_successor.println("QUERY_REQUEST;"+parts[1]);
								socket_get_from_successor.close();
							}

						}

						String returnKey = c1.getString(keyIndex);
						String returnValue = c1.getString(valueIndex);

						c1.moveToFirst();

						//while(!c1.isAfterLast())
						//{
						returnKey = c1.getString(keyIndex);
						returnValue = c1.getString(valueIndex);




						//String answer = keydump + "#" +valuedump;


						//REINITIALIZING FOR THIS RUN
						query_result="";
						//------------------------------------
						query_result = returnKey + ":"+ returnValue;


						//request = true;
						Log.e(TAG,"QUERY_RESULT SET = "+query_result);

						//ADDED 1MAY
						//flag=true;

						PrintWriter pout123 = new PrintWriter(socket.getOutputStream(), true);
						pout123.println(query_result);




					} // IF QUERY_REQUEST MESSAGE

					if(message.equals("*"))
					{
						Log.e(TAG,"* Message REQUEST SERVERTASK");

						//RETURN BACK ALL MY DB DATA

						SQLiteDatabase database = db.getWritableDatabase();

						Cursor c1 = database.rawQuery("SELECT * from simple_dynamo",null);

						String keydump = "";
						String valuedump = "";

						int keyIndex = c1.getColumnIndex("key");
						int valueIndex = c1.getColumnIndex("value");

						Log.e(TAG, "Keyindex " + keyIndex + "  " + valueIndex);

						c1.moveToFirst();

						String returnKey = c1.getString(keyIndex);
						String returnValue = c1.getString(valueIndex);

						c1.moveToFirst();

						while(!c1.isAfterLast())
						{
							returnKey = c1.getString(keyIndex);
							returnValue = c1.getString(valueIndex);

							keydump = keydump + ":" + returnKey;
							valuedump = valuedump + ":" + returnValue;

							c1.moveToNext();

						}


						String answer = keydump + "#" +valuedump;

						Log.e(TAG,"ANSWER 4-->"+answer);

						//query_dump_db = answer;
						//get_list = true;

						PrintWriter pout = new PrintWriter(socket.getOutputStream(), true);
						pout.println(answer);


					}


					if(message.equals("*_replica"))
					{
						Log.e(TAG," *_replica REQUEST SERVERTASK");

						//RETURN BACK ALL DB DATA WHICH BELONGS TO MY PREDECESSOR

						SQLiteDatabase database = db.getWritableDatabase();
						Cursor c1 = database.rawQuery("SELECT * from simple_dynamo",null);

						String keydump = "";
						String valuedump = "";

						int keyIndex = c1.getColumnIndex("key");
						int valueIndex = c1.getColumnIndex("value");

						//Log.e(TAG, "Keyindex " + keyIndex + "  " + valueIndex);

						c1.moveToFirst();

						String returnKey = c1.getString(keyIndex);
						String returnValue = c1.getString(valueIndex);

						c1.moveToFirst();

						int actual_position = -1;

						Log.e(TAG,"HEY THERE "+get_Predecessor(Integer.parseInt(myPort)));

						//FINDING POSITION OF PREDECESSOR NODE IN LIST
						for(int w=0;w<aliveList.size();w++)
						{
							Log.e(TAG,"D = "+w);
							Log.e(TAG,"CONVERT_BACK "+convert_back(aliveList.get(w)));

							if(Integer.parseInt(convert_back(aliveList.get(w))) == (get_Predecessor(Integer.parseInt(myPort))))
							{
								actual_position = w;
							}

						}

						Log.e(TAG,"ACTUAL LOCATION OF PREDECESSOR FAILED NODE IN LIST = "+actual_position);

						while(!c1.isAfterLast())
						{
							returnKey = c1.getString(keyIndex);
							returnValue = c1.getString(valueIndex);

							//-------


							//KEY SHOULD BE AT POSITION
							int returned_position = check(returnKey);
							Log.e(TAG,"KEY SHOULD BE AT POSITION "+returned_position);

							if(actual_position == returned_position)
							{
								//MEANS THIS KEY BELONGS TO NODE; SEND BACK
								keydump = keydump + ":" + returnKey;
								valuedump = valuedump + ":" + returnValue;

							}

							//------

							c1.moveToNext();

						}


						String answer = keydump + "#" +valuedump;

						Log.e(TAG,"ANSWER 5-->"+answer);

						//query_dump_db = answer;
						//get_list = true;

						PrintWriter pout = new PrintWriter(socket.getOutputStream(), true);
						pout.println(answer);


					}



				} // end of while

			}
			catch (Exception e) {
				Log.e("ServerTask", "Exception");
				e.printStackTrace();

			}


			return null;
		} // end of DoInBackground()
	}





	private class ClientTask extends AsyncTask<String, Void, Void> {

		private ObjectOutputStream outStream;
		private ObjectInputStream inStream;
		ObjectInputStream input;

		@Override
		protected  Void doInBackground(String... msgs) {


			try {

				Log.e(TAG, " IN CLIENTTASK msgs = " + msgs[0]);
				//String remotePort = msgs[1];


				if(msgs[0].contains("RECOVERED")) {

					int successor_of_recovered_node = get_Successor(Integer.parseInt(myPort));

					Log.e(TAG, "SUCCESSOR OF FAILED/RECOVERED NODE = " + successor_of_recovered_node);
					Log.e(TAG, "CLIENTTASK ASKING : " + successor_of_recovered_node + " IF I HAVE RECOVERED (TO SOMEONE ELSE)");

					Socket socket_recovered = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), successor_of_recovered_node);
					PrintWriter pw = new PrintWriter(socket_recovered.getOutputStream(), true);
					Log.e(TAG, "SENDING RECOVERED:" + myPort + " TO SERVERTASK");
					pw.println("RECOVERED:" + myPort);


					//RECEIVING BACK MISSED KEY-VALUE PAIRS FROM MY SUCCESSOR
					BufferedReader reply = new BufferedReader(new InputStreamReader(socket_recovered.getInputStream()));
					String reply_message = reply.readLine();

					aliveList.clear();

					//ADDED NEWLY

					aliveList.add("5562");
					aliveList.add("5556");
					aliveList.add("5554");
					aliveList.add("5558");
					aliveList.add("5560");


					//try{
					if (reply_message.contains("FILLER") || reply_message == "" || reply_message == null) {
						//DO NOTHING
						recover = true;
					}
					else {
						//MISSED KEY-VALUE PAIRS RECEIVED; ACTUAL RECOVERY CAUGHT

						recover = false;

						Log.e(TAG, "RECOVERY PROCESS STARTED FOR :" + myPort);
						Log.e(TAG, "RECOVERED RESPONSE " + reply_message);

						String[] reply_message_parts = reply_message.split("#");
						String reply_message_keys = reply_message_parts[0].substring(1);
						String reply_message_values = reply_message_parts[1].substring(1);

						String[] reply_message_keys_parts = reply_message_keys.split(":");
						String[] reply_message_values_parts = reply_message_values.split(":");

						for (int v = 0; v < reply_message_keys_parts.length; v++)
						{
							ContentValues cv = new ContentValues();
							cv.put("key", reply_message_keys_parts[v]);
							cv.put("value", reply_message_values_parts[v]);

							SQLiteDatabase database = db.getWritableDatabase();
							String[] selectionArgs = {reply_message_keys_parts[v]};

							Cursor cursor = database.query("simple_dynamo", null, "key = ?", selectionArgs, null, null, null);

							if (cursor.getCount() < 1) {
								database.insert("simple_dynamo", null, cv);

							} else {
								database.update("simple_dynamo", cv, "key = ?", selectionArgs); // if already existed values
							}

							Log.e("INSERTED MISSED ENTRIES", "and in content provider" + cv.toString());
							//	insertion = true;

						}

						// GET REPLICAS FROM 2 PREDECESSORS

						//1ST
						int pred = get_Predecessor(Integer.parseInt(myPort));
						Log.e(TAG, "PREDECESSOR OF FAILED NODE " + myPort + " IS " + pred);

						Socket socket_replica = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), pred);
						PrintWriter pw_replica = new PrintWriter(socket_replica.getOutputStream(), true);
						pw_replica.println("REPLICA_REQUEST");


						//RECEIVING BACK MISSED KEY-VALUE PAIRS FROM MY PREDECESSOR
						BufferedReader reply_replica = new BufferedReader(new InputStreamReader(socket_replica.getInputStream()));
						String reply_replica_message = reply_replica.readLine();


						//MISSED KEY-VALUE PAIRS RECEIVED; ACTUAL RECOVERY CAUGHT
						Log.e(TAG, "RECOVERED RESPONSE " + reply_replica_message);

						if (reply_replica_message == "#") {
						} else {


							String reply_with_ampersand = "";

							if (reply_replica_message.endsWith("&")) {
								reply_with_ampersand = reply_replica_message.substring(0, reply_replica_message.length() - 2);
							} else {
								reply_with_ampersand = reply_replica_message;
							}

							Log.e(TAG, "REPLY_WITH_AMPERSAND = " + reply_with_ampersand);

							String[] reply_from_2_preds = reply_with_ampersand.split("&");

							Log.e(TAG, "1st --> " + reply_from_2_preds[0]);
							Log.e(TAG, "2nd --> " + reply_from_2_preds[1]);

//-------------------------------------------------------------------------------------
							String[] rreply_message_parts = reply_from_2_preds[0].split("#");


							String rreply_message_keys = "";
							String rreply_message_values = "";

							if (rreply_message_parts[0].length() > 0)

							{
								rreply_message_keys = rreply_message_parts[0].substring(1);

								//if(rreply_message_parts[1].length()>0) {
								rreply_message_values = rreply_message_parts[1].substring(1);
								//}

								String[] rreply_message_keys_parts = rreply_message_keys.split(":");
								String[] rreply_message_values_parts = rreply_message_values.split(":");

								for (int v = 0; v < rreply_message_keys_parts.length; v++) {
									ContentValues cv = new ContentValues();
									cv.put("key", rreply_message_keys_parts[v]);
									cv.put("value", rreply_message_values_parts[v]);

									SQLiteDatabase database = db.getWritableDatabase();
									String[] selectionArgs = {rreply_message_keys_parts[v]};

									Cursor cursor = database.query("simple_dynamo", null, "key = ?", selectionArgs, null, null, null);

									if (cursor.getCount() < 1) {
										database.insert("simple_dynamo", null, cv);

									} else {
										database.update("simple_dynamo", cv, "key = ?", selectionArgs); // if already existed values
									}

									Log.e("INSERTED REPLICA ENTRIE", "and in content provider" + cv.toString());
									//	insertion = true;


								}


							}


							//2ND

							String[] rreply_message_parts123 = reply_from_2_preds[1].split("#");

							if (rreply_message_parts123.length > 0) {
								String rreply_message_keys123 = rreply_message_parts123[0].substring(1);
								String rreply_message_values123 = rreply_message_parts123[1].substring(1);

								String[] rreply_message_keys_parts123 = rreply_message_keys123.split(":");
								String[] rreply_message_values_parts123 = rreply_message_values123.split(":");

								for (int w = 0; w < rreply_message_keys_parts123.length; w++) {

									ContentValues cv123 = new ContentValues();
									cv123.put("key", rreply_message_keys_parts123[w]);
									cv123.put("value", rreply_message_values_parts123[w]);

									SQLiteDatabase database = db.getWritableDatabase();
									String[] selectionArgs123 = {rreply_message_keys_parts123[w]};

									Cursor cursor123 = database.query("simple_dynamo", null, "key = ?", selectionArgs123, null, null, null);

									if (cursor123.getCount() < 1) {
										database.insert("simple_dynamo", null, cv123);

									} else {
										database.update("simple_dynamo", cv123, "key = ?", selectionArgs123); // if already existed values
									}

									Log.e("INSERTED REPLICA 123", "and in content provider" + cv123.toString());


								}
							}


							socket_replica.close();

						}



/*						//2ND

						int pred_2 = get_Predecessor(pred);
						Log.e(TAG, "PREDECESSOR OF PREDECESSOR OF FAILED NODE " + pred + " IS " + pred_2);

						Socket socket_replica_2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), pred_2);
						PrintWriter pw_replica_2 = new PrintWriter(socket_replica_2.getOutputStream(), true);
						pw_replica_2.println("REPLICA_REQUEST");


						//RECEIVING BACK MISSED KEY-VALUE PAIRS FROM MY PREDECESSOR'S PREDECESSOR
						BufferedReader reply_replica_2 = new BufferedReader(new InputStreamReader(socket_replica_2.getInputStream()));
						String reply_replica_message_2 = reply_replica_2.readLine();


						//MISSED KEY-VALUE PAIRS RECEIVED; ACTUAL RECOVERY CAUGHT
						Log.e(TAG, "RECOVERED RESPONSE " + reply_replica_message_2);

						if(reply_replica_message_2 == "#"){}
						else {
							String[] rreply_message_parts2 = reply_replica_message_2.split("#");
							String rreply_message_keys2 = rreply_message_parts2[0].substring(1);
							String rreply_message_values2 = rreply_message_parts2[1].substring(1);

							String[] rreply_message_keys_parts2 = rreply_message_keys2.split(":");
							String[] rreply_message_values_parts2 = rreply_message_values2.split(":");

							for (int v = 0; v < rreply_message_keys_parts2.length; v++) {
								ContentValues cv = new ContentValues();
								cv.put("key", rreply_message_keys_parts2[v]);
								cv.put("value", rreply_message_values_parts2[v]);

								SQLiteDatabase database = db.getWritableDatabase();
								String[] selectionArgs = {rreply_message_keys_parts2[v]};

								Cursor cursor = database.query("simple_dynamo", null, "key = ?", selectionArgs, null, null, null);

								if (cursor.getCount() < 1)
								{
									database.insert("simple_dynamo", null, cv);

								}

								else
								{
									database.update("simple_dynamo", cv, "key = ?", selectionArgs); // if already existed values
								}

								Log.e("INSERTED 2nd REPLICAS", "and in content provider" + cv.toString());
							//	insertion = true;

								socket_replica_2.close();

							}
						}
*/


						Log.e(TAG,"PORT "+myPort+" HAS RECOVERED FULLY NOW");
						socket_recovered.close();

					} // end of inner ELSE

					//	}catch(	NullPointerException n)
					//		{
					//		Log.e(TAG,"null found!!");
					//	}



					recover = true;

				} // end of outer IF MESSAGE EQUALS



				if (msgs[0].equals("INITIALIZE LIST")) {
					Log.e(TAG, "INTIALIZE LIST");
					/*for (int i = 11108; i < 11126; i = i + 4)
					{
						Log.e(TAG, "SENDING INITIALIZE LIST TO:" + i);
						Socket socket_init = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), i);
						PrintWriter pw = new PrintWriter(socket_init.getOutputStream(), true);
						pw.println("INITIALIZE LIST");

						aliveList.add(convert(Integer.toString(i)));
						Log.e(TAG,"ADDED TO ALIVELIST : "+convert(Integer.toString(i)));

						socket_init.close();
					}

*/

					aliveList.clear();

					aliveList.add("5562");
					aliveList.add("5556");
					aliveList.add("5554");
					aliveList.add("5558");
					aliveList.add("5560");


					Log.e(TAG,"BEFORE SORTING LIST SIZE = "+aliveList.size());
					Collections.sort(aliveList,priorityComparator);

					Log.e(TAG,"DISPLAYING AFTER SORTING ");

					for(int t=0;t<aliveList.size();t++)
						Log.e(TAG," "+aliveList.get(t));
				}


			}
			catch(Exception e)
			{
				Log.e(TAG,"CAUGHT EXCEPTION IN INITIALIZE LIST");
				e.printStackTrace();
			}

			try
			{
				if(msgs[0].contains("INSERT")) // CODE IS NEVER COMING HERE NOW !! ISSUE HERE
				{
					Log.e(TAG,"INSERT MESSAGE IN CLIENTTASK");

					//-------------------------------------


					for (int i = 11108; i < 11126; i = i + 4)
					{

						//failed_node = -1;
						Log.e(TAG, "CHECKING FOR LIVENESS BEFORE INSERT:" + i);
						try {
							Socket socket_init = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), i);
							PrintWriter pw = new PrintWriter(socket_init.getOutputStream(), true);
							pw.println("aaa");

							//JUST TO MAKE IT WAIT FOR SOME TIME
							//int k=0;
							//while(k>25)
							//{
							//	k++;
							//}



							BufferedReader reply = new BufferedReader(new InputStreamReader(socket_init.getInputStream()));
							String reply_message = reply.readLine();

							if(reply_message == null || reply_message == "")
							{
								Log.e(TAG,"SETTING FAILED NODE = "+i);
								failed_node = i;

							}

							socket_init.close();
						} catch (SocketException se) {
							Log.e(TAG, "SOCKET EXCEPTION & SETTING FAILED NODE = " + i);
							failed_node = i;
							se.printStackTrace();
						} catch (IOException e) {
							Log.e(TAG, "IO EXCEPTION & SETTING FAILED NODE = "+i);
							failed_node = i;
							e.printStackTrace();
						}


					} // 	END OF FOR LOOP


					//BROADCAST THIS FAILED NODE MESSAGE TO ALL


					for (int i = 11108; i < 11126; i = i + 4)
					{
						Log.e(TAG, "CHECKING FOR LIVENESS BEFORE INSERT:" + i);

						Socket socket_i = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), i);
						PrintWriter pw = new PrintWriter(socket_i.getOutputStream(), true);
						pw.println("FAILED NODE:"+failed_node);
						socket_i.close();


					}

					//-----------------------------------

					String[] split = msgs[0].split(";");
					//Log.e(TAG,"FIRST = "+split[1]);

					//Log.e(TAG,"SPLIT[4] "+split[4]);

					//1ST
					String to_send = aliveList.get(Integer.parseInt(split[1]));
					Log.e(TAG,"TO SEND "+to_send);
					String converted_string = convert_back(to_send);
					Log.e(TAG,"SENDING TO "+Integer.parseInt(converted_string));

					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(converted_string));
					PrintWriter pout = new PrintWriter(socket.getOutputStream(), true);
					pout.println("INSERT_REQUEST;" + split[4]);

					//-----------



					///---------
					socket.close();

					// 2ND
					String to_send_2 = aliveList.get(Integer.parseInt(split[2]));
					Log.e(TAG,"TO SEND "+to_send_2);
					String converted_string_2 = convert_back(to_send_2);
					Log.e(TAG,"SENDING TO "+Integer.parseInt(converted_string_2));


					Socket socket_2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(converted_string_2));
					PrintWriter pout_2 = new PrintWriter(socket_2.getOutputStream(), true);
					pout_2.println("INSERT_REQUEST;" + split[4]);
///------------------------------------
//--------------------------------------
					socket_2.close();

					// 3RD
					String to_send_3 = aliveList.get(Integer.parseInt(split[3]));
					Log.e(TAG,"TO SEND "+to_send_3);
					String converted_string_3 = convert_back(to_send_3);
					Log.e(TAG,"SENDING TO "+Integer.parseInt(converted_string_3));


					Socket socket_3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(converted_string_3));
					PrintWriter pout_3 = new PrintWriter(socket_3.getOutputStream(), true);
					pout_3.println("INSERT_REQUEST;" + split[4]);

					//----------------------


					///--------------------



					socket_3.close();

				}

				if(msgs[0].contains("QUERY_REQUEST"))
				{
					Log.e(TAG,"QUERY REQUEST MESSAGE IN CLIENTTASK");

					String[] split = msgs[0].split(";");


					//IF QUERY REQUEST CANT GET BACK REPLY, THAT MEANS DESTINATION NODE HAS FAILED. SO ASK ITS REPLICA FOR KEY-VALUE PAIR

					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(split[2]));
					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					out.println("QUERY_REQUEST;"+split[1]);


					BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String message = br.readLine();

					Log.e(TAG,"QUERY_REPLY RECEIVED "+message);

					if (message == null)
					{
						//ASK SUCCESSOR WHO HAS REPLICA


						int replica_node = get_Successor(Integer.parseInt(split[2]));

						Log.e(TAG,"QUERYING REPLICA NODE FOR KEY-VALUE PAIR "+replica_node);

						Socket socket_replica_node = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), replica_node);
						PrintWriter out_replica_node = new PrintWriter(socket_replica_node.getOutputStream(), true);
						out_replica_node.println("QUERY_REQUEST;"+split[1]);


						BufferedReader br_replica_node = new BufferedReader(new InputStreamReader(socket_replica_node.getInputStream()));
						String message_from_replica_node = br_replica_node.readLine();

						Log.e(TAG,"QUERY_REPLY RECEIVED FROM REPLICA NODE "+message_from_replica_node);

						result_for_query = message_from_replica_node;
						flag = true;

						message_from_replica_node = "";

						socket_replica_node.close();




					}

					else
					{
						result_for_query = message;
						message = "";
						flag = true;


					}

					socket.close();

				}


				if(msgs[0].equals("REQUEST_ALL_DBS"))
				{
					Log.e(TAG,"REQUEST_ALL_DBS MESSAGE IN CLIENTTASK");



					for(int i=0;i<aliveList.size();i++)
					{
						//QUERY EVERYONE FOR THEIR DB DUMPS

						Log.e(TAG," -> "+convert_back(aliveList.get(i)));

						Socket socket_query = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(convert_back(aliveList.get(i))));
						PrintWriter pout_2 = new PrintWriter(socket_query.getOutputStream(), true);
						pout_2.println("*");


						BufferedReader iM = new BufferedReader(new InputStreamReader(socket_query.getInputStream()));
						String reply = iM.readLine();

						Log.e(TAG,"REPLY "+reply);


						if(reply == null)
						{
							// DESTINATION NODE HAS FAILED. SO QUERY ITS REPLICA NODE AND GET KEY-VALUE PAIRS WHICH BELONG TO IT.

							int replica_node_forStar = get_Successor(Integer.parseInt(convert_back(aliveList.get(i))));

							Log.e(TAG,"ASKING REPLICA FROM SUCCESSOR : "+replica_node_forStar);

							Socket socket_query_replica = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), replica_node_forStar);
							PrintWriter pout_2_replica = new PrintWriter(socket_query_replica.getOutputStream(), true);
							pout_2_replica.println("*_replica");


							BufferedReader iM_replica = new BufferedReader(new InputStreamReader(socket_query_replica.getInputStream()));
							String reply_fromReplica = iM_replica.readLine();

							Log.e(TAG,"REPLY FROM REPLICA "+reply_fromReplica);
							socket_query_replica.close();

							total_result = total_result + "!" + reply_fromReplica;

						}

						else
						{
							total_result = total_result + "!" + reply;
						}

						socket_query.close();

					}

					Log.e(TAG,"TOTAL RESULT ="+total_result);

					request = true;


				}

			}
			catch (UnknownHostException e) {
				Log.e("ClientTask", "UnknownHostException");
			}
			catch (IOException e) {
				Log.e("ClientTask", "Socket IOException");
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}

			return null;

		}



	}

	private class ClientTask_Query extends AsyncTask<String, Void , Cursor> {

		private ObjectOutputStream outStream;
		private ObjectInputStream inStream;
		ObjectInputStream input;
		MatrixCursor mcursor = null;

		@Override
		protected Cursor doInBackground(String... msgs) {


			try {

				if(msgs[0].contains("QUERY_REQUEST"))
				{


					String[] split = msgs[0].split(";");
					Log.e(TAG,"QUERY REQUEST MESSAGE IN CLIENTTASK_QUERY!!!! "+split[2]);

					//IF QUERY REQUEST CANT GET BACK REPLY, THAT MEANS DESTINATION NODE HAS FAILED. SO ASK ITS REPLICA FOR KEY-VALUE PAIR

					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(split[2]));
					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					out.println("QUERY_REQUEST;"+split[1]);


					BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String message = br.readLine();

					Log.e(TAG,"QUERY_REPLY RECEIVED "+message);

					if (message == null)
					{
						//ASK SUCCESSOR WHO HAS REPLICA


						int replica_node = get_Successor(Integer.parseInt(split[2]));

						Log.e(TAG,"QUERYING REPLICA NODE FOR KEY-VALUE PAIR "+replica_node);

						Socket socket_replica_node = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), replica_node);
						PrintWriter out_replica_node = new PrintWriter(socket_replica_node.getOutputStream(), true);
						out_replica_node.println("QUERY_REQUEST;"+split[1]);


						BufferedReader br_replica_node = new BufferedReader(new InputStreamReader(socket_replica_node.getInputStream()));
						String message_from_replica_node = br_replica_node.readLine();

						Log.e(TAG,"QUERY_REPLY RECEIVED FROM REPLICA NODE "+message_from_replica_node);

						result_for_query = message_from_replica_node;
						flag = true;

						message_from_replica_node = "";

						socket_replica_node.close();


						String final_result[] = result_for_query.split(":");
						Log.e(TAG, "AFTER UNLOCK " + final_result[0]+" "+final_result[1]);



						//String[] columns = final_result.split("#");

						//query_result=""; // RESETTING VARIABLE FOR NEXT USE

						Log.e(TAG, " -->" + final_result[0] + "-->" + final_result[1]);

						mcursor = new MatrixCursor(new String[]{"key", "value"});
						mcursor.moveToFirst();
						mcursor.addRow(new Object[]{final_result[0], final_result[1]});

						Log.e(TAG, "Key ->" + mcursor.getString(0));
						Log.e(TAG, "Value ->" + mcursor.getString(1));

						flag = false;
						//return mcursor;





					}

					else
					{
						result_for_query = message;
						message = "";
						flag = true;


						String final_result[] = result_for_query.split(":");
						Log.e(TAG, "AFTER UNLOCK " + final_result[0]+" "+final_result[1]);



						//String[] columns = final_result.split("#");

						//query_result=""; // RESETTING VARIABLE FOR NEXT USE

						Log.e(TAG, " -->" + final_result[0] + "-->" + final_result[1]);

						mcursor = new MatrixCursor(new String[]{"key", "value"});
						mcursor.moveToFirst();
						mcursor.addRow(new Object[]{final_result[0], final_result[1]});

						Log.e(TAG, "Key ->" + mcursor.getString(0));
						Log.e(TAG, "Value ->" + mcursor.getString(1));

						flag = false;
						//return mcursor;


					}

					socket.close();

				}









			} catch (Exception e) {
				e.printStackTrace();
			}

			Log.e(TAG,"CLIENTTASK_QUERY RETURNING MATRIXCURSOR TO QUERY()");

			return mcursor;
		}

		protected void onPostExecute(Cursor cursor) {
			super.onPostExecute(cursor);
		}

	}
	public synchronized void initialise_List()
	{

		Log.e(TAG,myPort + " is joining");
		Log.e(TAG,"INITIALIZE_LIST FOR :"+myPort);

		// 4 May 11 pm Changed from execute() to executeonExecutor
		new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"INITIALIZE LIST",myPort);





		/*//HARDCODING LIST
		aliveList.add("5554");
		aliveList.add("5556");
		aliveList.add("5558");
		aliveList.add("5560");
		aliveList.add("5562");
		*/
		//SORT
/*
		Log.e(TAG,"BEFORE SORTING LIST SIZE = "+aliveList.size());
		Collections.sort(aliveList,priorityComparator);

		Log.e(TAG,"DISPLAYING AFTER SORTING ");

		for(int t=0;t<aliveList.size();t++)
			Log.e(TAG," "+aliveList.get(t));
*/
	}


	public static synchronized String convert(String port)
	{

		if(port.equals("11108"))
			return "5554";
		else if(port.equals("11112"))
			return "5556";
		else if(port.equals("11116"))
			return "5558";
		else if(port.equals("11120"))
			return "5560";
		else
			return "5562";

	}

	public synchronized static String convert_back(String port)
	{

		if(port.equals("5554"))
			return "11108";
		else if(port.equals("5556"))
			return "11112";
		else if(port.equals("5558"))
			return "11116";
		else if(port.equals("5560"))
			return "11120";
		else
			return "11124";

	}


	public synchronized static  void decide_PnS()
	{
		Log.e(TAG,"DECIDE_PNS FOR ");

		for(int t=0;t<aliveList.size();t++)
		{

			String temp = convert(myPort);
			if(aliveList.get(t).equals(temp))
			{
				if(t == 0)
					predecessor = aliveList.get(aliveList.size()-1);
				else
					predecessor = aliveList.get(t-1);

				if(t == aliveList.size()-1)
					successor = aliveList.get(0);
				else
					successor = aliveList.get(t+1);
			}

		}

		Log.e(TAG,"SETTING PREDECESSOR = "+predecessor+" & SUCCESSOR = "+successor);

	}


	public synchronized static  int check(String key)
	{

		String r_key=key;
		try {

			String keyHash = genHash(r_key);
			Log.e(TAG,"CHECKING FOR KEY "+r_key+" & HASHED KEY = "+keyHash+"  "+aliveList.size());

			for(int z=0;z<aliveList.size();z++)
			{
				//SEARCH FOR WHICH LOCATION KEY SHUD BE INSERTED INTO
				String pHash = "";

				if(z == 0)
				{
					pHash = genHash(aliveList.get(aliveList.size()-1));
				}

				else
				{
					String g = aliveList.get(z-1);
					pHash = genHash(g);
				}

				String hash = genHash(aliveList.get(z));

				int result_1 = keyHash.compareTo(pHash);
				int result_2 = keyHash.compareTo(hash);

				//Log.e(TAG,"Z = " +z+ " & RESULT 1 = "+result_1+" & RESULT 2 = "+result_2);


				if(result_1 > 0 && result_2 <= 0)
				{
					//THEN MY KEY; INSERT
					return z;
				}

			} //END OF FOR LOOP


		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		return 0;
	}








	synchronized Cursor request_for_db()
	{
		Log.e(TAG, "IN REQUEST_FOR_DB :: REQUESTING EVERYONES DBS");

		Cursor cursor_db = null;
		MatrixCursor mcursor = new MatrixCursor(new String[]{"key", "value"});

		request = false;

		// 4 May 11 pm Changed from execute() to executeonExecutor
		new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"REQUEST_ALL_DBS", "11108");


		while(request!=true)
		{
			try {
				Thread.sleep(50); // 100
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(request == true)
				break;
		}

		request=false;


		//Log.e(TAG,"TOTAL RESULT IN REQUEST_FOR_DB -->"+total_result);
		total_result = total_result.substring(1);

		Log.e(TAG,"NEW TOTAL RESULT "+total_result);

		String[] outside = total_result.split("!");
		Log.e(TAG,"LENGTH "+outside.length);

		Log.e(TAG,"0 --> "+outside[0]);
		Log.e(TAG,"1 --> "+outside[1]);
		Log.e(TAG,"2 --> "+outside[2]);
		Log.e(TAG,"3 --> "+outside[3]);
		Log.e(TAG,"4 --> "+outside[4]);



		for(int j=0;j<outside.length;j++)
		{
			String[] one = outside[j].split("#");
			//one[0] keys ; one[1] values

			String[] keys = one[0].split(":");
			String[] values = one[1].split(":");

/*			Log.e(TAG, "ARUN");

			for (int u = 0; u < keys.length; u++)
				Log.e(TAG, " " + keys[u]);

			Log.e(TAG, "ARUN");

			for (int u = 0; u < values.length; u++)
				Log.e(TAG, " " + values[u]);
*/

			for (int r = 0; r < keys.length; r++) {
				String[] row = new String[]{keys[r], values[r]};
				mcursor.addRow(row);

			}

		}


		Log.e(TAG,"JUST BEFORE RETURNING CURSOR_DB");
		return mcursor;
	}

	public int get_Successor(int port)
	{
		if(port == 11108)
			return 11116;
		else if(port == 11112)
			return 11108;
		else if(port == 11116)
			return 11120;
		else if(port == 11120)
			return 11124;
		else
			return 11112;
	}



	public int get_Predecessor(int port)
	{
		if(port == 11108)
			return 11112;
		else if(port == 11112)
			return 11124;
		else if(port == 11116)
			return 11108;
		else if(port == 11120)
			return 11116;
		else
			return 11120;
	}


}
