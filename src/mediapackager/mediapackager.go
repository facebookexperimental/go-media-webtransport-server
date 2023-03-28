/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package mediapackager

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"jordicenzano/go-media-webtransport-server/memfile"
)

type PackagerVersion int

const (
	V1Json PackagerVersion = iota
	V2Binary
)

func VersionToString(version PackagerVersion) string {
	if version == V1Json {
		return "V1Json"
	}
	return "V2Binary"
}

func StringToVersion(versionStr string) PackagerVersion {
	if versionStr == "V1Json" {
		return V1Json
	}
	return V2Binary
}

func Decode(headerBytes []byte, decodedHeader *memfile.FileHeader) (version PackagerVersion, err error) {
	if headerBytes[0] == 0xff {
		return decodeV2(headerBytes, decodedHeader)
	} else {
		version = V1Json
		return decodeV1(headerBytes, decodedHeader)
	}
}

func Encode(header memfile.FileHeader, version PackagerVersion) (headerBytes []byte, err error) {
	if version == V2Binary {
		return encodeV2(header)
	} else {
		return encodeV1(header)
	}
}

// Internal functions

func encodeV2(header memfile.FileHeader) (headerBytes []byte, err error) {
	headerBytes = make([]byte, 1+1+4+8+8+8+8)

	pos := 0
	headerBytes[pos] = 0xff
	pos++

	headerBytes[pos] = 0x00 // Audio (0)
	if header.MediaType == "video" {
		headerBytes[pos] = headerBytes[pos] | 0b01000000
	}

	// delta (0)
	if header.ChunkType == "key" {
		headerBytes[pos] = headerBytes[pos] | 0b00010000
	}
	if header.ChunkType == "init" {
		headerBytes[pos] = headerBytes[pos] | 0b00100000
	}

	// Set valid duration, maxAge, seqId, timestamp
	headerBytes[pos] = headerBytes[pos] | 0b00001111
	pos++

	binary.BigEndian.PutUint32(headerBytes[pos:], uint32(memfile.GetMaxAgeFromCacheControlOr(header.CacheControl, -1)))
	pos += 4

	binary.BigEndian.PutUint64(headerBytes[pos:], uint64(header.SeqId))
	pos += 8

	binary.BigEndian.PutUint64(headerBytes[pos:], uint64(header.Timestamp))
	pos += 8

	binary.BigEndian.PutUint64(headerBytes[pos:], uint64(header.Duration))
	pos += 8

	binary.BigEndian.PutUint64(headerBytes[pos:], uint64(header.FirstFrameClk))

	return
}

func encodeV1(header memfile.FileHeader) (headerBytes []byte, err error) {
	ret, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	// Assuming encodes to UTF-8 (TODO: double check)
	return []byte(ret), nil
}

func decodeV1(headerBytes []byte, decodedHeader *memfile.FileHeader) (version PackagerVersion, err error) {
	version = V1Json
	err = json.Unmarshal(headerBytes, decodedHeader)
	return
}

func decodeV2(headerBytes []byte, decodedHeader *memfile.FileHeader) (version PackagerVersion, err error) {
	version = V2Binary
	buf := bytes.NewReader(headerBytes)

	var versionByte uint8
	errVersion := binary.Read(buf, binary.BigEndian, &versionByte)
	if errVersion != nil {
		err = errors.New(fmt.Sprintf("Header parsing version, err: %v", errVersion))
		return
	}
	if versionByte != 0xFF {
		err = errors.New(fmt.Sprintf("No recognized version byte, err: %v", errVersion))
		return
	}

	var dataByte uint8
	errDataByte := binary.Read(buf, binary.BigEndian, &dataByte)
	if errDataByte != nil {
		err = errors.New(fmt.Sprintf("Header parsing data byte, err: %v", errDataByte))
		return
	}
	if (dataByte>>6)&0b11 == 0 {
		decodedHeader.MediaType = "audio"
	} else if (dataByte>>6)&0b11 == 1 {
		decodedHeader.MediaType = "video"
	} else {
		err = errors.New(fmt.Sprintf("Header parse can not decode mediaType, dataByte: %d", dataByte))
	}

	if (dataByte>>4)&0b11 == 0 {
		decodedHeader.ChunkType = "delta"
	} else if (dataByte>>4)&0b11 == 1 {
		decodedHeader.ChunkType = "key"
	} else if (dataByte>>4)&0b11 == 2 {
		decodedHeader.ChunkType = "init"
	} else {
		err = errors.New(fmt.Sprintf("Header parse can not decode chunkType, dataByte: %d", dataByte))
	}

	validDuration := false
	if dataByte&0b00001000 > 0 {
		validDuration = true
	}
	validFirstFrameClk := false
	if dataByte&0b00000100 > 0 {
		validFirstFrameClk = true
	}
	validSeqId := false
	if dataByte&0b00000010 > 0 {
		validSeqId = true
	}
	validTimestamp := false
	if dataByte&0b00000001 > 0 {
		validTimestamp = true
	}

	var maxAgeS uint32
	errMaxAge := binary.Read(buf, binary.BigEndian, &maxAgeS)
	if errMaxAge != nil {
		err = errors.New(fmt.Sprintf("Header parsing maxAge, err: %v", errMaxAge))
		return
	}
	decodedHeader.CacheControl = fmt.Sprintf("max-age=%d", maxAgeS)

	if validSeqId {
		errSeqId := binary.Read(buf, binary.BigEndian, &decodedHeader.SeqId)
		if errSeqId != nil {
			err = errors.New(fmt.Sprintf("Header parsing seqId, err: %v", errSeqId))
			return
		}
	}

	if validTimestamp {
		errTimestamp := binary.Read(buf, binary.BigEndian, &decodedHeader.Timestamp)
		if errTimestamp != nil {
			err = errors.New(fmt.Sprintf("Header parsing timestamp, err: %v", errTimestamp))
			return
		}
	}

	if validDuration {
		errDuration := binary.Read(buf, binary.BigEndian, &decodedHeader.Duration)
		if errDuration != nil {
			err = errors.New(fmt.Sprintf("Header parsing duration, err: %v", errDuration))
			return
		}
	}

	if validFirstFrameClk {
		errFirstFrameClk := binary.Read(buf, binary.BigEndian, &decodedHeader.FirstFrameClk)
		if errFirstFrameClk != nil {
			err = errors.New(fmt.Sprintf("Header parsing FirstFrameClk, err: %v", errFirstFrameClk))
			return
		}
	}

	return
}
