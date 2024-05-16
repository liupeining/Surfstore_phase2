package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	RWMutex        sync.RWMutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// Retrieves the server's FileInfoMap
	// map<string, FileMetaData> fileInfoMap
	// string: used to get the member of FileInfoMap struct, I think.
	m.RWMutex.RLock()
	defer m.RWMutex.RUnlock()
	//type FileInfoMap struct {
	//	state         protoimpl.MessageState
	//	sizeCache     protoimpl.SizeCache
	//	unknownFields protoimpl.UnknownFields
	//
	//	FileInfoMap map[string]*FileMetaData `protobuf:"bytes,1,rep,name=fileInfoMap,proto3" json:"fileInfoMap,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	//}
	fileInfoMap := &FileInfoMap{
		FileInfoMap: make(map[string]*FileMetaData),
	}
	for k, v := range m.FileMetaMap {
		fileInfoMap.FileInfoMap[k] = v
	}
	return fileInfoMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.RWMutex.RLock()
	defer m.RWMutex.RUnlock()
	//message FileMetaData {
	//    string filename = 1;
	//    int32 version = 2;
	//    repeated string blockHashList = 3;
	//}
	_, exists := m.FileMetaMap[fileMetaData.Filename]
	if exists {
		curVersion := m.FileMetaMap[fileMetaData.Filename].Version
		if fileMetaData.Version > curVersion {
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		} else {
			return &Version{Version: -1}, nil
		}
	} else {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	}
	return &Version{Version: fileMetaData.Version}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	//message BlockStoreAddr {
	//    string addr = 1;
	//}
	m.RWMutex.RLock()
	defer m.RWMutex.RUnlock()
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
