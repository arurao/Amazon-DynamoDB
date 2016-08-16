package edu.buffalo.cse.cse486586.simpledynamo;
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

class MessengerOpenHelper extends SQLiteOpenHelper {


    //From http://developer.android.com/guide/topics/data/data-storage.html#filesExternal
    private static final int DATABASE_VERSION = 1;
    private static final String MESSENGER_TABLE_NAME = "simple_dynamo";
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private static final String MESSENGER_TABLE_CREATE =
            "CREATE TABLE " + MESSENGER_TABLE_NAME + " (" +
                    KEY_FIELD + " TEXT, " +
                    VALUE_FIELD + " TEXT);";

    public MessengerOpenHelper(Context context, String name,
                               SQLiteDatabase.CursorFactory factory, int version) {
        super(context, "simple_dynamo", factory, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS simple_dynamo");
        db.execSQL(MESSENGER_TABLE_CREATE);

    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS simple_dynamo");
        onCreate(db);
    }


}