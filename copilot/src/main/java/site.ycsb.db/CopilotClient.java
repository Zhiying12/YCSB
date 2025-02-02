package site.ycsb.db;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.codehaus.jackson.map.ObjectMapper;
import site.ycsb.*;

import java.io.*;
import java.net.Socket;
import java.util.*;

/**
 *
 */
public class CopilotClient extends DB {
  private int id;
  private Socket socket;
  private Config config;
  private DataOutputStream writer;
  private DataInputStream reader;
  private int leaderId;
  private List<Socket> sockets;
  private List<DataOutputStream> writers;
  private List<DataInputStream> readers;
  private int secondLeaderId;
  private int keySize;
  private int valueSize;
  private View[] views;
  private String defaultValue;
  private int opId;
  private Command cmd;
  private Propose propose;
  private BlockingQueue<Reply> replyQueue;
  private Thread[] threads;
  private static int clientId = -1;

  @Override
  public void init() throws DBException {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      config = objectMapper.readValue(
          new File("copilot.json"),
          Config.class);
    } catch (IOException e) {
      System.err.println("Couldn't load config.json");
      System.exit(1);
    }
//    id = UUID.randomUUID().hashCode() & 0xfffffff;
    leaderId = config.getLeaderId();
    secondLeaderId = config.getSecondLeaderId();
    sockets = new ArrayList<>();
    writers = new ArrayList<>();
    readers = new ArrayList<>();
    if (CopilotClient.clientId == -1) {
      CopilotClient.clientId = config.getClientId();
    }

    keySize = 0;
    valueSize = 500;
    opId = 0;

    views = new View[2];
    for (int i = 0; i < views.length; i++) {
      views[i] = new View();
      views[i].viewId = 0;
      views[i].active = true;
      views[i].pilotId = i;
    }
    views[0].replicaId = leaderId;
    views[1].replicaId = secondLeaderId;


    defaultValue = "a".repeat(500);

    cmd = new Command();
    cmd.clientId = CopilotClient.clientId;
    CopilotClient.clientId++;
    propose = new Propose();

    connect();

    // register client id
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(0, cmd.clientId);
    for (int i = 0; i < sockets.size(); i++) {
      try {
        DataOutputStream writer = writers.get(i);
        writer.write(MessageType.REGISTER_CLIENT_ID.getValue());
        writer.write(buffer.array());
        writer.flush();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    replyQueue = new LinkedBlockingQueue<>();

    threads = new Thread[sockets.size()];
    for (int i = 0; i < sockets.size(); i++) {
      int finalI = i;
      threads[i] = new Thread(() -> onReceive(finalI));
      threads[i].start();
    }
  }

  private void connect() {
    List<String> addresses = config.getAllServerAddresses();

    for (String address : addresses) {
      String[] tokens = address.split(":");
      String ip = tokens[0];
      int port = Integer.parseInt(tokens[1]);
      try {
        Socket s = new Socket(ip, port);
        s.setSoTimeout(2000);
        DataOutputStream w = new DataOutputStream(s.getOutputStream());
        DataInputStream r = new DataInputStream(s.getInputStream());
        sockets.add(s);
        writers.add(w);
        readers.add(r);
      } catch(Exception ignored) {
        //
      }
    }
    switchServer();
  }

  private void switchServer() {
    socket = sockets.get(leaderId);
    writer = writers.get(leaderId);
    reader = readers.get(leaderId);
  }

  //Read a single record
  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    if (keySize == 0) {
      keySize = key.length();
    }
    String newKey = keySizeCheck(key);
    cmd.op = OperationType.GET;
    cmd.key = newKey;
    cmd.value = defaultValue;
    opId++;
    cmd.opId = opId;
    propose.command = cmd;
    propose.commandId = opId;
    propose.timeStamp = Instant.now().getNano();;

    try {
      String value = sendRequest();
      result.put("field1", new StringByteIterator(value));
      return Status.OK;
    } catch (Exception e) {
//      e.printStackTrace();
      return Status.ERROR;
    }
  }

  //Perform a range scan
  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  //Update a single record
  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  //Insert a single record
  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    if (keySize == 0) {
      keySize = key.length();
    }
    String newKey = keySizeCheck(key);
    StringBuilder value = new StringBuilder();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String fieldVal = entry.getValue().toString();
      String modifiedVal = fieldVal.replace((char) 127, 'a');
      value.append(modifiedVal);
    }
    cmd.op = OperationType.PUT;
    cmd.key = newKey;
    cmd.value = value.toString();
    opId++;
    cmd.opId = opId;
    propose.command = cmd;
    propose.commandId = opId;
    propose.timeStamp = Instant.now().getNano();

    try {
      sendRequest();
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  //Delete a single record
  @Override
  public Status delete(final String table, final String key) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public void cleanup() {
    try {
      for (int i = 0; i < sockets.size(); i++) {
        sockets.get(i).close();
        readers.get(i).close();
        writers.get(i).close();
        threads[i].join();
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  private String sendRequest() throws Exception {
    boolean succeeded = false;
    while (true) {
      if (views[0].active) {
        leaderId = views[0].replicaId;
        try {
          sendProposal(leaderId);
          succeeded = true;
        } catch (Exception e) {
          e.printStackTrace();
          views[0].active = false;
          succeeded = false;
        }
      }
      if (!views[0].active) {
        leaderId = -1;
        int nextServerId = (views[0].replicaId + 1) % sockets.size();
        try {
          sendGetView(nextServerId, 0);
        } catch (Exception ignored) {
          //
        }
      }

      if (views[1].active) {
        secondLeaderId = views[1].replicaId;
        try {
          sendProposal(secondLeaderId);
          succeeded = true;
        } catch (Exception e) {
          e.printStackTrace();
          views[1].active = false;
          succeeded = false;
        }
      }
      if (!views[1].active) {
        secondLeaderId = -1;
        int nextServerId = (views[1].replicaId + 1) % sockets.size();
        try {
          sendGetView(nextServerId, 1);
        } catch (Exception ignored) {
          //
        }
      }

      if (!succeeded) {
        continue;
      }

      while (true) {
        Reply reply = replyQueue.poll(60, TimeUnit.SECONDS);
        if (reply != null) {
          byte msgType = reply.messageType.getValue();
          if (MessageType.values()[msgType] == MessageType.PROPOSE_REPLY) {
//          System.out.printf("reply id : %d, propose id: %d\n", reply.proposeReply.commandId, propose.commandId);
            if (reply.proposeReply.commandId == propose.commandId) {
              return reply.proposeReply.value;
            }
          } else {
            int pilotId = reply.getViewReply.pilotId;
            views[pilotId].pilotId = pilotId;
            views[pilotId].viewId = reply.getViewReply.viewId;
            views[pilotId].replicaId = reply.getViewReply.replicaId;
            views[pilotId].active = true;
          }
        } else {
          break;
        }
      }
    }
  }

  private void sendProposal(int serverId) throws Exception {
    DataOutputStream writer = writers.get(serverId);
    writer.writeByte(MessageType.PROPOSE.getValue());
    ByteBuffer buffer;
//    buffer = ByteBuffer.allocate(4);
//    buffer.order(ByteOrder.LITTLE_ENDIAN);
//    buffer.putInt(0, propose.commandId);
//    writer.write(buffer.array());
    int cmdId = propose.commandId;
    byte[] cmdByte = new byte[4];
    cmdByte[0] = (byte) cmdId;
    cmdByte[1] = (byte) (cmdId >> 8);
    cmdByte[2] = (byte) (cmdId >> 16);
    cmdByte[3] = (byte) (cmdId >> 24);
    writer.write(cmdByte);

    // send command
//    buffer.putInt(0, cmd.clientId);
//    writer.write(buffer.array());
//    buffer.putInt(0, cmd.opId);
//    writer.write(buffer.array());
    int cId = cmd.clientId;
    cmdByte[0] = (byte) cId;
    cmdByte[1] = (byte) (cId >> 8);
    cmdByte[2] = (byte) (cId >> 16);
    cmdByte[3] = (byte) (cId >> 24);
    writer.write(cmdByte);

    int opId = cmd.opId;
    cmdByte[0] = (byte) opId;
    cmdByte[1] = (byte) (opId >> 8);
    cmdByte[2] = (byte) (opId >> 16);
    cmdByte[3] = (byte) (opId >> 24);
    writer.write(cmdByte);
    writer.writeByte(cmd.op.getValue());
    writer.write(cmd.key.getBytes());
    writer.write(cmd.value.getBytes());

    // send timestamp
    buffer = ByteBuffer.allocate(8);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(0, propose.timeStamp);
    writer.write(buffer.array());
    writer.flush();
  }

  private void sendGetView(int serverId, int viewId) throws Exception {
    System.out.println("sent getview");
    DataOutputStream writer = writers.get(serverId);
    writer.writeByte(MessageType.GET_VIEW.getValue());
    ByteBuffer buffer;
    buffer = ByteBuffer.allocate(4);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(0, viewId);
    writer.write(buffer.array());
    writer.flush();
  }

  private ProposeReply recvProposeReply(int serverId) throws Exception {
    DataInputStream reader = readers.get(serverId);
    ProposeReply proposeReply = new ProposeReply();
    proposeReply.OK = reader.readByte();
    byte[] buffer = new byte[4];
    int n = reader.read(buffer, 0, 4);
    if (n != 4) {
      System.out.printf("not correct number expect 4, but got %d\n", n);
    }
//    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
//    byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
//    proposeReply.commandId = byteBuffer.getInt();
    proposeReply.commandId = (((buffer[3] & 0xff    ) << 24) |
        ((buffer[2] & 0xff) << 16) |
        ((buffer[1] & 0xff) <<  8) |
        ((buffer[0] & 0xff)      ));
    buffer = new byte[valueSize];
    reader.readFully(buffer);
    proposeReply.value = new String(buffer);
    buffer = new byte[8];
    n = reader.read(buffer, 0, 8);
    if (n != 8) {
      System.out.printf("not correct number expect 8, but got %d\n", n);
    }
    return proposeReply;
  }

  private GetViewReply recvGetViewReply(int serverId) throws Exception {
    DataInputStream reader = readers.get(serverId);
    GetViewReply getViewReply = new GetViewReply();
    getViewReply.OK = reader.readByte();
    getViewReply.viewId = reader.readInt();
    getViewReply.pilotId = reader.readInt();
    getViewReply.replicaId = reader.readInt();
    return getViewReply;
  }

  private void onReceive(int serverId) {
    Reply reply = new Reply();
    DataInputStream reader = readers.get(serverId);
    byte msgType;
    while (true) {
      try {
        msgType = reader.readByte();
        if (MessageType.values()[msgType] == MessageType.PROPOSE_REPLY) {
          reply.proposeReply = recvProposeReply(serverId);
          reply.messageType = MessageType.PROPOSE_REPLY;
        } else {
          reply.getViewReply = recvGetViewReply(serverId);
          reply.messageType = MessageType.GET_VIEW_REPLY;
        }
        replyQueue.put(reply);
      } catch (Exception e) {
//        e.printStackTrace();
        break;
      }
    }
  }

  String keySizeCheck(String key) {
    int len = key.length();
    if (len < keySize) {
      for (int i = len; i < keySize; i++) {
        key += '1';
      }
    } else if (len > keySize) {
      key = key.substring(0, 23);
    }
    return key;
  }

  void tmp() {
    try {
      DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      ByteBuffer buffer = ByteBuffer.allocate(4);
      buffer.order(ByteOrder.LITTLE_ENDIAN);
      buffer.putInt(12345678);
      out.write(buffer.array());
      writer.write(0);
      writer.flush();
//      out.write(4);
      out.write("testtesthij".getBytes());
      out.write("abcdabcdefg".getBytes());
      out.flush();
    } catch (IOException ignored) {
      //
    }
  }
}
