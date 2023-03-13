/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package memfiles

import (
	"errors"
	"jordicenzano/go-media-webtransport-server/deliverysession"
	"jordicenzano/go-media-webtransport-server/memfile"
	"math"
	"path/filepath"
	"strings"
	"time"
)

// Return newest asset after session started and inside session window that has not been sent yet
func (mfs *MemFiles) GetFileForAssetNewestSeqId(assetID string, mediaType string, sessionWindow time.Duration, deliverySession *deliverysession.DeliverySession) (fret *memfile.MemFile) {
	// TODO: Optimize this with indexes

	presfixStr := filepath.Join(assetID, mediaType)
	var maxSeqId int64 = -1
	now := time.Now()

	mfs.filesLock.RLock()
	defer mfs.filesLock.RUnlock()

	for key, file := range mfs.dataMap {
		if strings.HasPrefix(key, presfixStr) {
			// Received after session started
			if deliverySession.GetStartedAt().Before(file.ReceivedAt) {
				// Inside session window
				if now.Add(-sessionWindow).Before(file.ReceivedAt) {
					// Not sent yet
					if !deliverySession.IsSentSeqId(mediaType, file.Headers.SeqId) {
						// Highest seqId
						if file.Headers.SeqId > maxSeqId {
							maxSeqId = file.Headers.SeqId
							fret = file
						}
					}
				} else {
					// Clean up
					// It is outside the window, not useful. Clean up to avoid OOM
					deliverySession.RemoveSentSeqId(mediaType, file.Headers.SeqId)
				}
			}
		}
	}

	if fret != nil && fret.Headers.SeqId >= 0 {
		deliverySession.AddSentSeqId(mediaType, fret.Headers.SeqId)
	}
	return
}

// First call return highest seqId that was received < (now - rewind)
// Returns lowestSeqId NOT sent yet that was captured between (sessionStarted + time) and (sessionStarted + time + sessionWindow)
func (mfs *MemFiles) GetNextByTimeSeqId(assetID string, mediaType string, sessionWindow time.Duration, rewind time.Duration, deliverySession *deliverysession.DeliverySession) (fret *memfile.MemFile) {
	// TODO: Optimize this with indexes

	prefixStr := filepath.Join(assetID, mediaType)
	now := time.Now()

	mfs.filesLock.RLock()
	defer mfs.filesLock.RUnlock()

	isFirst := deliverySession.GetStartedAtClk().IsZero()
	var nextSeqIdToSent int64 = math.MaxInt64
	var minSeqId int64 = math.MaxInt64
	var sessionDur time.Duration
	var sendWindowLow time.Time
	var sendWindowHigh time.Time
	if isFirst {
		nextSeqIdToSent = -1
	} else {
		sessionDur = time.Since(deliverySession.GetStartedAt())
		sendWindowLow = deliverySession.GetStartedAtClk().Add(sessionDur)
		sendWindowHigh = deliverySession.GetStartedAtClk().Add(sessionDur).Add(sessionWindow)
	}

	for key, file := range mfs.dataMap {
		if strings.HasPrefix(key, prefixStr) {
			if isFirst {
				// Get 1st seqId after rewind
				if now.Add(-rewind).After(file.ReceivedAt) {
					if file.Headers.SeqId > nextSeqIdToSent && file.Headers.SeqId >= 0 {
						nextSeqIdToSent = file.Headers.SeqId
						fret = file
					}
				}
				if file.Headers.SeqId < minSeqId && file.Headers.SeqId >= 0 {
					minSeqId = file.Headers.SeqId
				}
			} else {
				// Find next seqId
				frameCapturedAt := time.UnixMilli(file.Headers.FirstFrameClk)
				if frameCapturedAt.After(sendWindowLow) && frameCapturedAt.Before(sendWindowHigh) {
					if !deliverySession.IsSentSeqId(mediaType, file.Headers.SeqId) {
						if file.Headers.SeqId < nextSeqIdToSent && file.Headers.SeqId >= 0 {
							nextSeqIdToSent = file.Headers.SeqId
							fret = file
						}
					}
				} else {
					// Clean up
					// It is outside the window, not useful. Clean up to avoid OOM
					deliverySession.RemoveSentSeqId(mediaType, file.Headers.SeqId)
				}
			}
		}
	}

	// If is using rewind and it can NOT find any 1st item just sent the minimum seqID (start of live)
	if isFirst && fret == nil && minSeqId < math.MaxInt64 {
		fret, _ = mfs.getFile(assetID, mediaType, false, minSeqId, false)
	}

	if fret != nil && fret.Headers.SeqId >= 0 {
		if isFirst {
			deliverySession.SetStartedAtClk(fret.Headers.FirstFrameClk)
		}
		deliverySession.AddSentSeqId(mediaType, fret.Headers.SeqId)
	}

	return
}

// First call return highest seqId that was received < startAt
// Returns lowestSeqId NOT sent yet that was captured between (sessionStarted + time) and (sessionStarted + time + sessionWindow)
func (mfs *MemFiles) GetNextByStartEnd(assetID string, mediaType string, sessionWindow time.Duration, startAt time.Time, endAt time.Time, deliverySession *deliverysession.DeliverySession) (fret *memfile.MemFile, err error) {
	// TODO: Optimize this with indexes

	prefixStr := filepath.Join(assetID, mediaType)

	mfs.filesLock.RLock()
	defer mfs.filesLock.RUnlock()

	isFirst := deliverySession.GetStartedAtClk().IsZero()
	var nextSeqIdToSent int64 = math.MaxInt64
	var minSeqId int64 = math.MaxInt64
	var sessionDur time.Duration
	var sendWindowLow time.Time
	var sendWindowHigh time.Time
	if isFirst {
		nextSeqIdToSent = -1
	} else {
		sessionDur = time.Since(deliverySession.GetStartedAt())
		sendWindowLow = deliverySession.GetStartedAtClk().Add(sessionDur)
		sendWindowHigh = deliverySession.GetStartedAtClk().Add(sessionDur).Add(sessionWindow)
	}

	for key, file := range mfs.dataMap {
		if strings.HasPrefix(key, prefixStr) {
			if isFirst {
				// Get last seqId before start
				if startAt.After(time.UnixMilli(file.Headers.FirstFrameClk)) {
					if file.Headers.SeqId > nextSeqIdToSent && file.Headers.SeqId >= 0 {
						nextSeqIdToSent = file.Headers.SeqId
						fret = file
					}
				}
				if file.Headers.SeqId < minSeqId && file.Headers.SeqId >= 0 {
					minSeqId = file.Headers.SeqId
				}
			} else {
				// Find next seqId
				frameCapturedAt := time.UnixMilli(file.Headers.FirstFrameClk)
				if frameCapturedAt.After(sendWindowLow) && frameCapturedAt.Before(sendWindowHigh) {
					if !deliverySession.IsSentSeqId(mediaType, file.Headers.SeqId) {
						if file.Headers.SeqId < nextSeqIdToSent && file.Headers.SeqId >= 0 {
							nextSeqIdToSent = file.Headers.SeqId
							fret = file
						}
					}
				} else {
					// Clean up
					// It is outside the window, not useful. Clean up to avoid OOM
					deliverySession.RemoveSentSeqId(mediaType, file.Headers.SeqId)
				}
			}
		}
	}

	// If is using VOD and it can NOT find any 1st item just sent the minimum seqID (start of live)
	if isFirst && fret == nil && minSeqId < math.MaxInt64 {
		fret, _ = mfs.getFile(assetID, mediaType, false, minSeqId, false)
	}

	if fret != nil && fret.Headers.SeqId >= 0 {
		if isFirst {
			deliverySession.SetStartedAtClk(fret.Headers.FirstFrameClk)
		} else {
			if endAt.Before(time.UnixMilli(fret.Headers.FirstFrameClk)) {
				// Detected VOD end
				err = errors.New("EOS")
			}
		}
		deliverySession.AddSentSeqId(mediaType, fret.Headers.SeqId)
	}

	return
}
