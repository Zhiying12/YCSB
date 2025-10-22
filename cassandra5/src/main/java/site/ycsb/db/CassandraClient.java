package site.ycsb.db;

import site.ycsb.*;

import java.util.*;

import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.cql.*;

public class CassandraClient extends DB {

  // --- Properties keys ---
  private static final String PROP_HOSTS = "cassandra.hosts";
  private static final String PROP_PORT  = "cassandra.port";
  private static final String PROP_LOCAL_DC = "cassandra.localdc";
  private static final String PROP_USERNAME  = "cassandra.username";
  private static final String PROP_PASSWORD  = "cassandra.password";
  private static final String PROP_SSL       = "cassandra.ssl";
  private static final String PROP_KEYSPACE  = "cassandra.keyspace";
  private static final String PROP_TABLE     = "cassandra.table";
  private static final String PROP_REPL      = "cassandra.replication";
  private static final String PROP_AUTO_SCHEMA = "cassandra.auto_create_schema";
  private static final String PROP_USE_BATCH = "cassandra.use_batch";

  private CqlSession session;
  private String keyspace;
  private String table;
  private ConsistencyLevel readCl, writeCl;
  private boolean useBatch;

  // Prepared statements
  private PreparedStatement psReadAll;
  private PreparedStatement psInsert;
  private PreparedStatement psUpdate;

  // YCSB field model
  private int fieldCount = 10;
  private boolean readAllFields = true;

  @Override
  public void init() throws DBException {
    Properties p = getProperties();

    String[] hosts = p.getProperty(PROP_HOSTS, "127.0.0.1").split(",");
    int port = Integer.parseInt(p.getProperty(PROP_PORT, "9042").trim());
    String localDc = p.getProperty(PROP_LOCAL_DC, "datacenter1");

    String username = p.getProperty(PROP_USERNAME, "cassandra").trim();
    String password = p.getProperty(PROP_PASSWORD, "cassandra").trim();

    this.keyspace  = p.getProperty(PROP_KEYSPACE, "ycsb");
    this.table     = p.getProperty(PROP_TABLE, "usertable");
    String repl    = p.getProperty(PROP_REPL, "{\'class\':\'SimpleStrategy\',\'replication_factor\':\'1\'}");
    boolean autoCreateSchema = Boolean.parseBoolean(p.getProperty(PROP_AUTO_SCHEMA, "true"));
    this.useBatch  = Boolean.parseBoolean(p.getProperty(PROP_USE_BATCH, "false"));

    this.readCl = ConsistencyLevel.ALL;
    this.writeCl = ConsistencyLevel.ALL;

    CqlSessionBuilder builder = CqlSession.builder().withLocalDatacenter(localDc);
    for (String h : hosts) {
      String[] hp = h.trim().split(":");
      String host = hp[0].trim();
      int prt = (hp.length > 1) ? Integer.parseInt(hp[1].trim()) : port;
      builder = builder.addContactPoint(new java.net.InetSocketAddress(host, prt));
    }
    if (!username.isEmpty()) {
      builder = builder.withAuthCredentials(username, password);
    }

    try {
      session = builder.build();
    } catch (Exception e) {
      throw new DBException("Failed to create CqlSession", e);
    }

    if (autoCreateSchema) {
      createSchemaIfNeeded(session, keyspace, table, repl);
    }

    // Prepare statements
    psReadAll = session.prepare(String.format(
        "SELECT * FROM %s.%s WHERE ycsb_key = ?",
        CqlIdentifier.fromCql(keyspace).asCql(true), CqlIdentifier.fromCql(table).asCql(true)));

    // dynamic projection handled by building query string at runtime when necessary

    // INSERT and UPDATE leverage the field count
    StringBuilder cols = new StringBuilder("ycsb_key");
    StringBuilder qs   = new StringBuilder("?");
    for (int i = 0; i < fieldCount; i++) {
      cols.append(", field").append(i);
      qs.append(", ?");
    }
    psInsert = session.prepare(String.format(
        "INSERT INTO %s.%s (%s) VALUES (%s) IF NOT EXISTS",
        CqlIdentifier.fromCql(keyspace).asCql(true), CqlIdentifier.fromCql(table).asCql(true), cols, qs));

    StringBuilder set = new StringBuilder();
    for (int i = 0; i < fieldCount; i++) {
      if (i > 0) set.append(", ");
      set.append("field").append(i).append("=?");
    }
    psUpdate = session.prepare(String.format(
        "UPDATE %s.%s SET %s WHERE ycsb_key = ? IF EXISTS",
        CqlIdentifier.fromCql(keyspace).asCql(true), CqlIdentifier.fromCql(table).asCql(true), set));
  }

  @Override
  public void cleanup() throws DBException {
    if (session != null) session.close();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      Row row;
      if (readAllFields || fields == null) {
        row = session.execute(psReadAll.bind(key).setConsistencyLevel(readCl)).one();
      } else {
        // Build a projected SELECT for requested fields + key
        String cols = "ycsb_key," + String.join(",", fields);
        SimpleStatement stmt = SimpleStatement.newInstance(
                String.format("SELECT %s FROM %s.%s WHERE ycsb_key=?", cols,
                    CqlIdentifier.fromCql(keyspace).asCql(true), CqlIdentifier.fromCql(this.table).asCql(true)), key)
            .setConsistencyLevel(readCl);
        row = session.execute(stmt).one();
      }
      if (row == null) return Status.NOT_FOUND;

      // populate result map
      if (readAllFields || fields == null) {
        for (int i = 0; i < fieldCount; i++) {
          String col = "field" + i;
          String v = row.getString(col);
          if (v != null) result.put(col, new StringByteIterator(v));
        }
      } else {
        for (String col : fields) {
          String v = row.getString(col);
          if (v != null) result.put(col, new StringByteIterator(v));
        }
      }
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
      final Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      BoundStatementBuilder b = psInsert.boundStatementBuilder().setConsistencyLevel(writeCl);
      b = b.setString(0, key);
      for (int i = 0; i < fieldCount; i++) {
        String col = "field" + i;
        String val = values.containsKey(col) ? values.get(col).toString() : null;
        b = b.setString(i + 1, val);
      }
      if (useBatch) {
        BatchStatement batch = BatchStatement.builder(DefaultBatchType.UNLOGGED).addStatement(b.build()).build();
        session.execute(batch);
      } else {
        session.execute(b.build());
      }
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      BoundStatementBuilder b = psUpdate.boundStatementBuilder().setConsistencyLevel(writeCl);
      for (int i = 0; i < fieldCount; i++) {
        String col = "field" + i;
        String val = values.containsKey(col) ? values.get(col).toString() : null;
        b = b.setString(i, val); // fields first
      }
      b = b.setString(fieldCount, key); // WHERE ycsb_key=?
      if (useBatch) {
        BatchStatement batch = BatchStatement.builder(DefaultBatchType.UNLOGGED).addStatement(b.build()).build();
        session.execute(batch);
      } else {
        session.execute(b.build());
      }
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    return Status.NOT_IMPLEMENTED;
  }

  private static void createSchemaIfNeeded(CqlSession session, String keyspace, String table, String replJson) {
    session.execute("CREATE KEYSPACE IF NOT EXISTS " + CqlIdentifier.fromCql(keyspace).asCql(true)
        + " WITH replication = " + replJson);

    StringBuilder cols = new StringBuilder("ycsb_key text PRIMARY KEY");
    for (int i = 0; i < 10; i++) {
      cols.append(", field").append(i).append(" text");
    }
    session.execute("CREATE TABLE IF NOT EXISTS " + CqlIdentifier.fromCql(keyspace).asCql(true) + "."
        + CqlIdentifier.fromCql(table).asCql(true) + " (" + cols + ")");
  }
}
