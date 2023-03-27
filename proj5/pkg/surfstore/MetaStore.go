package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fileName := fileMetaData.Filename
	version := fileMetaData.Version
	_, ok := m.FileMetaMap[fileName]
	if ok {
		if version == m.FileMetaMap[fileName].Version+1 {
			m.FileMetaMap[fileName] = fileMetaData
		} else {
			err := fmt.Errorf("version mismatch in server")
			return nil, err
		}

	} else {
		m.FileMetaMap[fileName] = fileMetaData
	}
	return &Version{Version: version}, nil

}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := make(map[string]*BlockHashes)
	blockHashes := blockHashesIn.Hashes
	for _, blockHash := range blockHashes {
		serverName := m.ConsistentHashRing.GetResponsibleServer(blockHash)
		_, ok := blockStoreMap[serverName]
		if !ok {
			blockHashesOut := new(BlockHashes)
			blockHashesOut.Hashes = make([]string, 0)
			blockStoreMap[serverName] = blockHashesOut
		}

		blockStoreMap[serverName].Hashes = append(blockStoreMap[serverName].Hashes, blockHash)

	}
	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil

}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {

	consistentHashRing := NewConsistentHashRing(blockStoreAddrs)

	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: consistentHashRing,
	}
}
