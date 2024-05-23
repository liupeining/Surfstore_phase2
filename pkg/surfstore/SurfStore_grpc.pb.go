// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v5.26.1
// source: pkg/surfstore/SurfStore.proto

package surfstore

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	BlockStore_GetBlock_FullMethodName       = "/surfstore.BlockStore/GetBlock"
	BlockStore_PutBlock_FullMethodName       = "/surfstore.BlockStore/PutBlock"
	BlockStore_MissingBlocks_FullMethodName  = "/surfstore.BlockStore/MissingBlocks"
	BlockStore_GetBlockHashes_FullMethodName = "/surfstore.BlockStore/GetBlockHashes"
)

// BlockStoreClient is the client API for BlockStore service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type BlockStoreClient interface {
	GetBlock(ctx context.Context, in *BlockHash, opts ...grpc.CallOption) (*Block, error)
	PutBlock(ctx context.Context, in *Block, opts ...grpc.CallOption) (*Success, error)
	MissingBlocks(ctx context.Context, in *BlockHashes, opts ...grpc.CallOption) (*BlockHashes, error)
	GetBlockHashes(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*BlockHashes, error)
}

type blockStoreClient struct {
	cc grpc.ClientConnInterface
}

func NewBlockStoreClient(cc grpc.ClientConnInterface) BlockStoreClient {
	return &blockStoreClient{cc}
}

func (c *blockStoreClient) GetBlock(ctx context.Context, in *BlockHash, opts ...grpc.CallOption) (*Block, error) {
	out := new(Block)
	err := c.cc.Invoke(ctx, BlockStore_GetBlock_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blockStoreClient) PutBlock(ctx context.Context, in *Block, opts ...grpc.CallOption) (*Success, error) {
	out := new(Success)
	err := c.cc.Invoke(ctx, BlockStore_PutBlock_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blockStoreClient) MissingBlocks(ctx context.Context, in *BlockHashes, opts ...grpc.CallOption) (*BlockHashes, error) {
	out := new(BlockHashes)
	err := c.cc.Invoke(ctx, BlockStore_MissingBlocks_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blockStoreClient) GetBlockHashes(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*BlockHashes, error) {
	out := new(BlockHashes)
	err := c.cc.Invoke(ctx, BlockStore_GetBlockHashes_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BlockStoreServer is the server API for BlockStore service.
// All implementations must embed UnimplementedBlockStoreServer
// for forward compatibility
type BlockStoreServer interface {
	GetBlock(context.Context, *BlockHash) (*Block, error)
	PutBlock(context.Context, *Block) (*Success, error)
	MissingBlocks(context.Context, *BlockHashes) (*BlockHashes, error)
	GetBlockHashes(context.Context, *emptypb.Empty) (*BlockHashes, error)
	mustEmbedUnimplementedBlockStoreServer()
}

// UnimplementedBlockStoreServer must be embedded to have forward compatible implementations.
type UnimplementedBlockStoreServer struct {
}

func (UnimplementedBlockStoreServer) GetBlock(context.Context, *BlockHash) (*Block, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetBlock not implemented")
}
func (UnimplementedBlockStoreServer) PutBlock(context.Context, *Block) (*Success, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PutBlock not implemented")
}
func (UnimplementedBlockStoreServer) MissingBlocks(context.Context, *BlockHashes) (*BlockHashes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MissingBlocks not implemented")
}
func (UnimplementedBlockStoreServer) GetBlockHashes(context.Context, *emptypb.Empty) (*BlockHashes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetBlockHashes not implemented")
}
func (UnimplementedBlockStoreServer) mustEmbedUnimplementedBlockStoreServer() {}

// UnsafeBlockStoreServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to BlockStoreServer will
// result in compilation errors.
type UnsafeBlockStoreServer interface {
	mustEmbedUnimplementedBlockStoreServer()
}

func RegisterBlockStoreServer(s grpc.ServiceRegistrar, srv BlockStoreServer) {
	s.RegisterService(&BlockStore_ServiceDesc, srv)
}

func _BlockStore_GetBlock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BlockHash)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlockStoreServer).GetBlock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BlockStore_GetBlock_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlockStoreServer).GetBlock(ctx, req.(*BlockHash))
	}
	return interceptor(ctx, in, info, handler)
}

func _BlockStore_PutBlock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Block)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlockStoreServer).PutBlock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BlockStore_PutBlock_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlockStoreServer).PutBlock(ctx, req.(*Block))
	}
	return interceptor(ctx, in, info, handler)
}

func _BlockStore_MissingBlocks_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BlockHashes)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlockStoreServer).MissingBlocks(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BlockStore_MissingBlocks_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlockStoreServer).MissingBlocks(ctx, req.(*BlockHashes))
	}
	return interceptor(ctx, in, info, handler)
}

func _BlockStore_GetBlockHashes_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlockStoreServer).GetBlockHashes(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BlockStore_GetBlockHashes_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlockStoreServer).GetBlockHashes(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

// BlockStore_ServiceDesc is the grpc.ServiceDesc for BlockStore service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var BlockStore_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "surfstore.BlockStore",
	HandlerType: (*BlockStoreServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetBlock",
			Handler:    _BlockStore_GetBlock_Handler,
		},
		{
			MethodName: "PutBlock",
			Handler:    _BlockStore_PutBlock_Handler,
		},
		{
			MethodName: "MissingBlocks",
			Handler:    _BlockStore_MissingBlocks_Handler,
		},
		{
			MethodName: "GetBlockHashes",
			Handler:    _BlockStore_GetBlockHashes_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/surfstore/SurfStore.proto",
}

const (
	MetaStore_GetFileInfoMap_FullMethodName     = "/surfstore.MetaStore/GetFileInfoMap"
	MetaStore_UpdateFile_FullMethodName         = "/surfstore.MetaStore/UpdateFile"
	MetaStore_GetBlockStoreMap_FullMethodName   = "/surfstore.MetaStore/GetBlockStoreMap"
	MetaStore_GetBlockStoreAddrs_FullMethodName = "/surfstore.MetaStore/GetBlockStoreAddrs"
)

// MetaStoreClient is the client API for MetaStore service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type MetaStoreClient interface {
	GetFileInfoMap(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*FileInfoMap, error)
	UpdateFile(ctx context.Context, in *FileMetaData, opts ...grpc.CallOption) (*Version, error)
	GetBlockStoreMap(ctx context.Context, in *BlockHashes, opts ...grpc.CallOption) (*BlockStoreMap, error)
	GetBlockStoreAddrs(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*BlockStoreAddrs, error)
}

type metaStoreClient struct {
	cc grpc.ClientConnInterface
}

func NewMetaStoreClient(cc grpc.ClientConnInterface) MetaStoreClient {
	return &metaStoreClient{cc}
}

func (c *metaStoreClient) GetFileInfoMap(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*FileInfoMap, error) {
	out := new(FileInfoMap)
	err := c.cc.Invoke(ctx, MetaStore_GetFileInfoMap_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *metaStoreClient) UpdateFile(ctx context.Context, in *FileMetaData, opts ...grpc.CallOption) (*Version, error) {
	out := new(Version)
	err := c.cc.Invoke(ctx, MetaStore_UpdateFile_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *metaStoreClient) GetBlockStoreMap(ctx context.Context, in *BlockHashes, opts ...grpc.CallOption) (*BlockStoreMap, error) {
	out := new(BlockStoreMap)
	err := c.cc.Invoke(ctx, MetaStore_GetBlockStoreMap_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *metaStoreClient) GetBlockStoreAddrs(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*BlockStoreAddrs, error) {
	out := new(BlockStoreAddrs)
	err := c.cc.Invoke(ctx, MetaStore_GetBlockStoreAddrs_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MetaStoreServer is the server API for MetaStore service.
// All implementations must embed UnimplementedMetaStoreServer
// for forward compatibility
type MetaStoreServer interface {
	GetFileInfoMap(context.Context, *emptypb.Empty) (*FileInfoMap, error)
	UpdateFile(context.Context, *FileMetaData) (*Version, error)
	GetBlockStoreMap(context.Context, *BlockHashes) (*BlockStoreMap, error)
	GetBlockStoreAddrs(context.Context, *emptypb.Empty) (*BlockStoreAddrs, error)
	mustEmbedUnimplementedMetaStoreServer()
}

// UnimplementedMetaStoreServer must be embedded to have forward compatible implementations.
type UnimplementedMetaStoreServer struct {
}

func (UnimplementedMetaStoreServer) GetFileInfoMap(context.Context, *emptypb.Empty) (*FileInfoMap, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetFileInfoMap not implemented")
}
func (UnimplementedMetaStoreServer) UpdateFile(context.Context, *FileMetaData) (*Version, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateFile not implemented")
}
func (UnimplementedMetaStoreServer) GetBlockStoreMap(context.Context, *BlockHashes) (*BlockStoreMap, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetBlockStoreMap not implemented")
}
func (UnimplementedMetaStoreServer) GetBlockStoreAddrs(context.Context, *emptypb.Empty) (*BlockStoreAddrs, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetBlockStoreAddrs not implemented")
}
func (UnimplementedMetaStoreServer) mustEmbedUnimplementedMetaStoreServer() {}

// UnsafeMetaStoreServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to MetaStoreServer will
// result in compilation errors.
type UnsafeMetaStoreServer interface {
	mustEmbedUnimplementedMetaStoreServer()
}

func RegisterMetaStoreServer(s grpc.ServiceRegistrar, srv MetaStoreServer) {
	s.RegisterService(&MetaStore_ServiceDesc, srv)
}

func _MetaStore_GetFileInfoMap_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MetaStoreServer).GetFileInfoMap(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MetaStore_GetFileInfoMap_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MetaStoreServer).GetFileInfoMap(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _MetaStore_UpdateFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FileMetaData)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MetaStoreServer).UpdateFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MetaStore_UpdateFile_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MetaStoreServer).UpdateFile(ctx, req.(*FileMetaData))
	}
	return interceptor(ctx, in, info, handler)
}

func _MetaStore_GetBlockStoreMap_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BlockHashes)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MetaStoreServer).GetBlockStoreMap(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MetaStore_GetBlockStoreMap_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MetaStoreServer).GetBlockStoreMap(ctx, req.(*BlockHashes))
	}
	return interceptor(ctx, in, info, handler)
}

func _MetaStore_GetBlockStoreAddrs_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MetaStoreServer).GetBlockStoreAddrs(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MetaStore_GetBlockStoreAddrs_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MetaStoreServer).GetBlockStoreAddrs(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

// MetaStore_ServiceDesc is the grpc.ServiceDesc for MetaStore service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var MetaStore_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "surfstore.MetaStore",
	HandlerType: (*MetaStoreServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetFileInfoMap",
			Handler:    _MetaStore_GetFileInfoMap_Handler,
		},
		{
			MethodName: "UpdateFile",
			Handler:    _MetaStore_UpdateFile_Handler,
		},
		{
			MethodName: "GetBlockStoreMap",
			Handler:    _MetaStore_GetBlockStoreMap_Handler,
		},
		{
			MethodName: "GetBlockStoreAddrs",
			Handler:    _MetaStore_GetBlockStoreAddrs_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/surfstore/SurfStore.proto",
}
