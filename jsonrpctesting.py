import time
import json
import requests
import pymongo
import os
import pdb
import tqdm


DB_NAME = "Ethereum_Blockchain"
COLLECTION = "Transactions"

def _rpcRequest(method, params, url='http://localhost:8545', 
                headers={'Content-type': 'application/json'}, key='result', delay = .0001, callid=0):
    """Make an RPC request to geth on port 8545."""
    payload = {
        "method": method,
        "params": params,
        "jsonrpc": "2.0",
        "id": callid
    }
    time.sleep(delay)
    res = requests.post(
          url,
          data=json.dumps(payload),
          headers=headers).json()
    return res[key]

# Geth
# ----
def decodeBlock(block):
    """
    Decode various pieces of information (from hex) for a block and return the parsed data.

    Note that the block is of the form:
 	{
       "id": 0,
    	"jsonrpc": "2.0",
    	"result": {
    	  "number": "0xf4241",
    	  "hash": "0xcb5cab7266694daa0d28cbf40496c08dd30bf732c41e0455e7ad389c10d79f4f",
    	  "parentHash": "0x8e38b4dbf6b11fcc3b9dee84fb7986e29ca0a02cecd8977c161ff7333329681e",
    	  "nonce": "0x9112b8c2b377fbe8",
    	  "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
    	  "logsBloom": "0x0",
    	  "transactionsRoot": "0xc61c50a0a2800ddc5e9984af4e6668de96aee1584179b3141f458ffa7d4ecec6",
    	  "stateRoot": "0x7dd4aabb93795feba9866821c0c7d6a992eda7fbdd412ea0f715059f9654ef23",
    	  "receiptRoot": "0xb873ddefdb56d448343d13b188241a4919b2de10cccea2ea573acf8dbc839bef",
    	  "miner": "0x2a65aca4d5fc5b5c859090a6c34d164135398226",
    	  "difficulty": "0xb6b4bbd735f",
    	  "totalDifficulty": "0x63056041aaea71c9",
    	  "size": "0x292",
    	  "extraData": "0xd783010303844765746887676f312e352e31856c696e7578",
    	  "gasLimit": "0x2fefd8",
    	  "gasUsed": "0x5208",
    	  "timestamp": "0x56bfb41a",
    	  "transactions": [
    	    {
    	      "hash": "0xefb6c796269c0d1f15fdedb5496fa196eb7fb55b601c0fa527609405519fd581",
    	      "nonce": "0x2a121",
    	      "blockHash": "0xcb5cab7266694daa0d28cbf40496c08dd30bf732c41e0455e7ad389c10d79f4f",
    	      "blockNumber": "0xf4241",
    	      "transactionIndex": "0x0",
    	      "from": "0x2a65aca4d5fc5b5c859090a6c34d164135398226",
    	      "to": "0x819f4b08e6d3baa33ba63f660baed65d2a6eb64c",
    	      "value": "0xe8e43bc79c88000",
    	      "gas": "0x15f90",
    	      "gasPrice": "0xba43b7400",
    	      "input": "0x"
    	    }
    	  ],
    	  "uncles": []
    	}
  	}
    """
    try:
        b = block
        if "result" in block:
            b = block["result"]
        # Filter the block
        new_block = {
            "number": int(b["number"], 16),
            "timestamp": int(b["timestamp"], 16),		# Timestamp is in unix time
            "transactions": []
        }
        # Filter and decode each transaction and add it back
        # 	Value, gas, and gasPrice are all converted to ether
        for t in b["transactions"]:
            new_t = {
                "from": t["from"],
                "to": t["to"],
                "value": float(int(t["value"], 16))/1000000000000000000.,
                "data": t["input"]
            }
            new_block["transactions"].append(new_t)
        return new_block

    except:
        return None
    
def getBlock(n):
    """Get a specific block from the blockchain and filter the data."""
    data = _rpcRequest("eth_getBlockByNumber", [hex(n), True])
    block = decodeBlock(data)
    return block

# mongodb
# -------
def initMongo(client):
    """
    Given a mongo client instance, create db/collection if either doesn't exist

    Parameters:
    -----------
    client <mongodb Client>

    Returns:
    --------
    <mongodb Client>
    """
    db = client[DB_NAME]
    try:
        db.create_collection(COLLECTION)
    except:
        pass
    try:
        # Index the block number so duplicate records cannot be made
        db[COLLECTION].create_index(
			[("number", pymongo.DESCENDING)],
			unique=True
		)
    except:
        pass

    return db[COLLECTION]

from collections import deque
import pdb


DB_NAME = "Ethereum_Blockchain"
COLLECTION = "Transactions"

# mongodb
# -------
def initMongo(client):
    """
    Given a mongo client instance, create db/collection if either doesn't exist

    Parameters:
    -----------
    client <mongodb Client>

    Returns:
    --------
    <mongodb Client>
    """
    db = client[DB_NAME]
    try:
        db.create_collection(COLLECTION)
    except:
        pass
    try:
        # Index the block number so duplicate records cannot be made
        db[COLLECTION].create_index(
			[("number", pymongo.DESCENDING)],
			unique=True
		)
    except:
        pass

    return db[COLLECTION]

def insertMongo(client, d):
    """
    Insert a document into mongo client with collection selected.

    Params:
    -------
    client <mongodb Client>
    d <dict>

    Returns:
    --------
    error <None or str>
    """
    try:
        client.insert_one(d)
        return None
    except Exception as err:
        pass
    
def saveBlock(client, block):
        """Insert a given parsed block into mongo."""
        e = insertMongo(client, block)
        if e:
            client.insertion_errors.append(e)
            
            
def highestBlock(client):
    """
    Get the highest numbered block in the collection.

    Params:
    -------
    client <mongodb Client>

    Returns:
    --------
    <int>
    """
    n = client.find_one(sort=[("number", pymongo.DESCENDING)])
    if not n:
        # If the database is empty, the highest block # is 0
        return 0
    assert "number" in n, "Highest block is incorrectly formatted"
    return n["number"]

def highestBlockMongo(client):
    """Find the highest numbered block in the collection."""
    highest_block = highestBlock(client)
    return highest_block

def add_block(client, n):
    """Add a block to mongo."""
    b = getBlock(n)
    if b:
        saveBlock(client, b)
        time.sleep(0.001)
    else:
        saveBlock(client, {"number": n, "transactions": []})

def highestBlockEth():
    """Find the highest numbered block in geth."""
    block = _rpcRequest("eth_syncing", [])
    return int(block['currentBlock'],16)

def updateParsedEthereumChain(client):
    """Initiate with MongoClient"""
    db = initMongo(client)
    highestEthblock = highestBlockEth()
    highestMongoblock = highestBlockMongo(db)
    for number in tqdm.tqdm(range(highestMongoblock, highestEthbloc+1)):
        add_block(db, number)
    print("Done! Highest block is now {}".format(highestBlockMongo(db)))
