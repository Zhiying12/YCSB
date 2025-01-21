package site.ycsb.db;

import java.nio.ByteBuffer;

enum OperationType {
  PUT((byte) 1),
  GET((byte) 2),
  DEL((byte) 3);

  private final byte value;

  OperationType(byte value) {
    this.value = value;
  }

  public byte getValue() {
    return value;
  }
}

enum MessageType {
  PROPOSE((byte) 0),
  PROPOSE_REPLY((byte) 1),
  READ((byte) 2),
  READ_REPLY((byte) 3),
  PROPOSE_AND_READ((byte) 4),
  PROPOSE_AND_READ_REPLY((byte) 5),
  GENERIC_SMR_BEACON((byte) 6),
  GENERIC_SMR_BEACON_REPLY((byte) 7),
  REGISTER_CLIENT_ID((byte) 8),
  REGISTER_CLIENT_ID_REPLY((byte) 9),
  GET_VIEW((byte) 10),
  GET_VIEW_REPLY((byte) 11);

  private final byte value;

  MessageType(byte value) {
    this.value = value;
  }

  public byte getValue() {
    return value;
  }
}

class Command {
  public int clientId;
  public int opId;
  public OperationType op;
  public String key;
  public String value;
}

class Propose {
  public int commandId;
  public Command command;
  public long timeStamp;
}

class ProposeReply {
  public byte OK;
  public int commandId;
  public String value;
  public long timestamp;
}

class GetView {
  public int pilotId;
}

class GetViewReply {
  public byte OK;
  public int viewId;
  public int pilotId;
  public int replicaId;
}

class Reply {
  public MessageType messageType;
  public ProposeReply proposeReply;
  public GetViewReply getViewReply;
}

class RegisterClientIdArgs {
  public int clientId;
}
