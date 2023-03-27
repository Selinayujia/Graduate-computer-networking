package surfstore

import (
	context "context"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	s, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = s.Flag
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = b.Hashes
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for {
		for addr := range surfClient.MetaStoreAddrs {
			conn, err := grpc.Dial(surfClient.MetaStoreAddrs[addr], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			c := NewRaftSurfstoreClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			f, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
			if err != nil {
				errStatus, _ := status.FromError(err)
				if errStatus.Message() == ERR_SERVER_CRASHED.Error() || errStatus.Message() == ERR_NOT_LEADER.Error() {
					conn.Close()
					continue
				}
				conn.Close()
				return err
			}
			*serverFileInfoMap = f.FileInfoMap
			return conn.Close()
		}
	}
}
func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for {
		for addr := range surfClient.MetaStoreAddrs {
			conn, err := grpc.Dial(surfClient.MetaStoreAddrs[addr], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			c := NewRaftSurfstoreClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			version, err := c.UpdateFile(ctx, fileMetaData)
			if err != nil {
				errStatus, _ := status.FromError(err)
				if errStatus.Message() == ERR_SERVER_CRASHED.Error() || errStatus.Message() == ERR_NOT_LEADER.Error() {
					conn.Close()
					continue
				}
				conn.Close()
				return err
			}
			if version != nil {
				*latestVersion = version.Version

			}
			return conn.Close()
		}
	}
}

/*
func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewMetaStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	*blockStoreAddr = addr.Addr
	return conn.Close()
}
*/

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {

	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {

		conn.Close()
		return err
	}
	*blockHashes = b.Hashes
	return conn.Close()

}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {

	for {
		for addr := range surfClient.MetaStoreAddrs {
			blockHashesInObj := BlockHashes{Hashes: blockHashesIn}
			conn, err := grpc.Dial(surfClient.MetaStoreAddrs[addr], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			c := NewRaftSurfstoreClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			blockStoreMapObj, err := c.GetBlockStoreMap(ctx, &blockHashesInObj)

			if err != nil {
				errStatus, _ := status.FromError(err)
				if errStatus.Message() == ERR_SERVER_CRASHED.Error() || errStatus.Message() == ERR_NOT_LEADER.Error() {
					conn.Close()
					continue
				}
				conn.Close()
				return err
			}
			hashesMap := blockStoreMapObj.BlockStoreMap

			resultMap := make(map[string][]string)
			for blockServer, blockHashObj := range hashesMap {
				resultMap[blockServer] = blockHashObj.Hashes
			}
			*blockStoreMap = resultMap

			return conn.Close()
		}
	}
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for {
		for addr := range surfClient.MetaStoreAddrs {
			conn, err := grpc.Dial(surfClient.MetaStoreAddrs[addr], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			c := NewRaftSurfstoreClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			addrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
			if err != nil {
				errStatus, _ := status.FromError(err)
				if errStatus.Message() == ERR_SERVER_CRASHED.Error() || errStatus.Message() == ERR_NOT_LEADER.Error() {
					conn.Close()
					continue
				}
				conn.Close()
				return err
			}
			*blockStoreAddrs = addrs.BlockStoreAddrs
			return conn.Close()

		}
	}
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
