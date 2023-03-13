/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package memfiles

import (
	"errors"
	"fmt"
	"jordicenzano/go-media-webtransport-server/memfile"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// File Definition of files
type MemFiles struct {
	dataMap map[string]*memfile.MemFile

	// FilesLock Lock used to write / read files
	filesLock *sync.RWMutex

	// Housekeeping thread channel
	cleanUpChannel chan bool
}

// New Creates a new mem files map
func New(housekeepingPeriodMs int64) *MemFiles {
	fs := MemFiles{dataMap: map[string]*memfile.MemFile{}, filesLock: new(sync.RWMutex), cleanUpChannel: make(chan bool)}

	if housekeepingPeriodMs > 0 {
		fs.startCleanUp(housekeepingPeriodMs)
	}

	return &fs
}

func (mfs *MemFiles) Stop() {
	mfs.stopCleanUp()
}

// Add empty file
// Cache key format assetId/mediaType/(seqId OR init)
func (mfs *MemFiles) AddNewEmptyFile(assetID string, mediaType string, isInit bool, seqId int64, headers memfile.FileHeader) *memfile.MemFile {
	cacheKey := getCacheKey(assetID, mediaType, isInit, seqId)

	newFile := memfile.New(headers)
	mfs.filesLock.Lock()
	defer mfs.filesLock.Unlock()
	mfs.dataMap[cacheKey] = newFile
	return newFile
}

func (mfs *MemFiles) GetFile(assetID string, mediaType string, isInit bool, seqId int64) (fRet *memfile.MemFile, errRet error) {
	return mfs.getFile(assetID, mediaType, isInit, seqId, true)
}

// Helpers

func (mfs *MemFiles) getFile(assetID string, mediaType string, isInit bool, seqId int64, lock bool) (fRet *memfile.MemFile, errRet error) {
	cacheKey := getCacheKey(assetID, mediaType, isInit, seqId)

	if lock {
		mfs.filesLock.RLock()
	}
	fRet, ok := mfs.dataMap[cacheKey]
	if lock {
		mfs.filesLock.RUnlock()
	}

	if !ok {
		errRet = errors.New("not found: " + cacheKey)
		return nil, errRet
	}
	return fRet, errRet
}

func getCacheKey(assetID string, mediaType string, isInit bool, seqId int64) string {
	cacheKey := ""
	if isInit {
		cacheKey = assetID + "/" + mediaType + "/init"
	} else {
		cacheKey = assetID + "/" + mediaType + "/" + strconv.FormatInt(seqId, 10)
	}
	return cacheKey
}

// Housekeeping

func (mfs *MemFiles) startCleanUp(periodMs int64) {
	go mfs.runCleanupEvery(periodMs, mfs.cleanUpChannel)

	log.Info("Started clean up thread")
}

func (mfs *MemFiles) stopCleanUp() {
	// Send finish signal
	mfs.cleanUpChannel <- true

	// Wait to finish
	<-mfs.cleanUpChannel

	log.Info("Stopped clean up thread")
}

func (mfs *MemFiles) runCleanupEvery(periodMs int64, cleanUpChannelBidi chan bool) {
	timeCh := time.NewTicker(time.Millisecond * time.Duration(periodMs))
	exit := false

	for !exit {
		select {
		// Wait for the next tick
		case tm := <-timeCh.C:
			mfs.cacheCleanUp(tm)

		case <-cleanUpChannelBidi:
			exit = true
		}
	}
	// Indicates finished
	cleanUpChannelBidi <- true

	log.Info("Exited clean up thread")
}

func (mfs *MemFiles) cacheCleanUp(now time.Time) {
	filesToDel := map[string]*memfile.MemFile{}

	// TODO: This is a brute force approach, optimization recommended

	mfs.filesLock.Lock()
	defer mfs.filesLock.Unlock()

	numStartElememts := len(mfs.dataMap)

	// Check for expired files
	for key, file := range mfs.dataMap {
		if file.MaxAgeS >= 0 && file.Eof {
			if file.ReceivedAt.Add(time.Second * time.Duration(file.MaxAgeS)).Before(now) {
				filesToDel[key] = file
			}
		}
	}
	// Delete expired files
	for keyToDel := range filesToDel {
		// Delete from array
		delete(mfs.dataMap, keyToDel)
		log.Info("CLEANUP expired, deleted: ", keyToDel)
	}

	numEndElememts := len(mfs.dataMap)

	log.Info(fmt.Sprintf("Finished cleanup round expired. Elements at start: %d, elements at end: %d", numStartElememts, numEndElememts))
}
