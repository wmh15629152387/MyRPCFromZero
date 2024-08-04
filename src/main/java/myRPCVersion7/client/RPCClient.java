package myRPCVersion7.client;


import myRPCVersion7.common.RPCRequest;
import myRPCVersion7.common.RPCResponse;

public interface RPCClient {
    RPCResponse sendRequest(RPCRequest request);
    void createWatch(String path);

}
