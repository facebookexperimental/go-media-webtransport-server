/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/kixelated/quic-go/http3"
	"github.com/kixelated/webtransport-go"

	"jordicenzano/go-media-webtransport-server/deliverysession"
	"jordicenzano/go-media-webtransport-server/mediapackager"
	"jordicenzano/go-media-webtransport-server/memfile"
	"jordicenzano/go-media-webtransport-server/memfiles"
)

// Delivery: Types of sessions
const (
	DeliverySessionLiveEdge   string = "edge"
	DeliverySessionLiveRewind        = "rewind"
	DeliverySessionVOD               = "vod"
)

const CACHE_CLEAN_UP_PERIOD_MS = 10000
const NO_MORE_FRAMES_WAIT_MS = 5
const COPY_BLOCK_BYTES = 2048
const HEADER_SIZE_LIMIT_BYTES = 500 * 1024 // Header size limit

// Delivery: Sent data up to Xms before live edge pointer
const DELIVERY_SESSION_WINDOW_DEFAULT_MS = 300

// Delivery: Kill delivery session of inflight request is higher than this
const MAX_INFLIGHT_REQUEST = 300

// Delivery: Max inflight request before stop sending video
const MAX_INFLIGHT_REQUEST_BEFORE_STOP_VIDEO = 30

// Delivery: Audio will have this priority over video
const AUDIO_OFFSET_SEND_PRIORITY = math.MaxInt / 2

// Delivery: Cancel request after (0 means NO cancel)
const LIVE_CANCEL_AFTER_TIMES_JITTER = 0

func readBytes(s *webtransport.ReceiveStream, buffer []byte) error {
	readSize := 0
	totalSize := len(buffer)
	var err error = nil
	for readSize < totalSize && err == nil {
		n := 0
		tmpBuffer := make([]byte, totalSize-readSize)
		n, err = io.ReadFull(*s, tmpBuffer)
		copy(buffer[readSize:readSize+n], tmpBuffer[0:n])

		readSize += n
	}
	return err
}

func handleWebTransportIngestStreams(session *webtransport.Session, ingestSessionID string, urlPath string, urlQS url.Values, memFiles *memfiles.MemFiles) {
	// Get asset ID from URL path
	assetID := ""
	pathElements := strings.Split(urlPath, "/")
	if len(pathElements) >= 3 {
		assetID = pathElements[2]
	}

	if assetID == "" {
		log.Error(fmt.Sprintf("%s - Session closed, we could NOT parse asset ID from URL path", ingestSessionID))
		return
	}

	// Handle incoming unidirectional streams
	go func() {
		for {
			rxStream, errAccUni := session.AcceptUniStream(session.Context())
			if errAccUni != nil {
				log.Error(fmt.Sprintf("%s - Session closed, not accepting more uni streams: %v", ingestSessionID, errAccUni))
				break
			}
			log.Info(fmt.Sprintf("%s(%v) - Accepting incoming uni stream", ingestSessionID, rxStream.StreamID()))

			go func(s webtransport.ReceiveStream) {
				isError := false
				headersSizeBytes := make([]byte, 8)
				errReadHeaderSize := readBytes(&s, headersSizeBytes)
				if errReadHeaderSize != nil {
					log.Error(fmt.Sprintf("%s(%v) - Error trying to read header length from uni stream. Err: %v", ingestSessionID, s.StreamID(), errReadHeaderSize))
					isError = true
				}

				header := memfile.FileHeader{}
				if !isError {
					headersSize := binary.BigEndian.Uint64(headersSizeBytes)
					log.Info(fmt.Sprintf("%s(%v) - Reading %d bytes of headers", ingestSessionID, s.StreamID(), headersSize))
					if headersSize >= HEADER_SIZE_LIMIT_BYTES {
						log.Error(fmt.Sprintf("%s(%v) - Error the header size is above allowed threshold. Requested (bytes): %d, Max allowed(bytes): %d", ingestSessionID, s.StreamID(), headersSize, HEADER_SIZE_LIMIT_BYTES))
						isError = true
					} else {
						headerBytes := make([]byte, headersSize)
						errReadHeaderSize := readBytes(&s, headerBytes)
						if errReadHeaderSize != nil {
							log.Error(fmt.Sprintf("%s(%v) - Error trying to read header from uni stream. Err: %v", ingestSessionID, s.StreamID(), errReadHeaderSize))
							isError = true
						}
						if !isError {
							version, errPackager := mediapackager.Decode(headerBytes, &header)
							if errPackager != nil {
								log.Error(fmt.Sprintf("%s(%v) - Error trying to parse header from uni stream. Contents: %s. Err: %v", ingestSessionID, s.StreamID(), headerBytes, errPackager))
								isError = true
							}
							log.Info(fmt.Sprintf("%s(%v) - Header decoded %s: %v", ingestSessionID, s.StreamID(), mediapackager.VersionToString(version), header))
						}
					}
				}

				if !isError {
					mediaType, seqId, isInit := getAssetInfo(header)
					f := memFiles.AddNewEmptyFile(assetID, mediaType, isInit, seqId, header)

					log.Info(fmt.Sprintf("%s(%v) - New file added. AssetId: %s, path: %s/%d", ingestSessionID, s.StreamID(), assetID, mediaType, seqId))

					for {
						buf := make([]byte, COPY_BLOCK_BYTES)
						n, err := s.Read(buf)
						if err != nil && err != io.EOF {
							log.Error(fmt.Sprintf("%s(%v) - Error reading from uni stream. Err: %v", ingestSessionID, s.StreamID(), err))
							isError = true
							break
						}

						f.Write(buf[:n])

						log.Info(fmt.Sprintf("%s(%v) - Copied from uni stream. Size: %d", ingestSessionID, s.StreamID(), n))

						if err == io.EOF {
							log.Info(fmt.Sprintf("%s(%v) - End of stream", ingestSessionID, s.StreamID()))
							break
						}
					}
					if isError {
						f.CloseNotFinished()
					} else {
						f.Close()
					}
				}
			}(rxStream)
		}
	}()
}

func handleWebTransportDeliveryStreams(session *webtransport.Session, deliverySessionID string, urlPath string, urlQS url.Values, memFiles *memfiles.MemFiles) {
	// Get asset ID from URL path
	assetID := ""
	pathElements := strings.Split(urlPath, "/")
	if len(pathElements) >= 3 {
		assetID = pathElements[2]
	}

	if assetID == "" {
		log.Error(fmt.Sprintf("%s - Session closed, we could NOT parse asset ID from URL path", deliverySessionID))
		session.CloseWithError(1, "Problem parsing session params")
		return
	}

	// Handle incoming unidirectional streams
	go func() {
		var inFlightReq int32 = 0

		// Parse session QS data
		rewindMs, videoJitterMs, audioJitterMs, startedAt, endAt, packagerVersion := parseWTQSData(urlQS)

		sessionType := getDeliverySessionType(rewindMs, startedAt, endAt)

		// Create delivery session
		deliverySession := deliverysession.New(assetID)

		// Send audio init
		audioInitf, errGettingAudioInit := memFiles.GetFile(assetID, "audio", true, 0)
		if errGettingAudioInit != nil {
			log.Error(fmt.Sprintf("%s - Problem getting audio init for delivery uni stream, err: %v", deliverySessionID, errGettingAudioInit))
			session.CloseWithError(1, "Problem getting audio init")
			return
		}
		errAIniSend := sendFile(session, deliverySessionID, &inFlightReq, audioInitf, packagerVersion, math.MaxInt, 0)
		if errAIniSend != nil {
			log.Error(fmt.Sprintf("%s - Problem sending audio init for delivery uni stream, err: %v", deliverySessionID, errAIniSend))
			session.CloseWithError(1, "Problem sending audio init")
			return
		}

		// Send video init
		videoInitf, errGettingVideoInit := memFiles.GetFile(assetID, "video", true, 0)
		if errGettingVideoInit != nil {
			log.Error(fmt.Sprintf("%s - Problem sending video init for delivery uni stream, err: %v", deliverySessionID, errGettingVideoInit))
			session.CloseWithError(1, "Problem getting video init")
			return
		}
		errVInisend := sendFile(session, deliverySessionID, &inFlightReq, videoInitf, packagerVersion, math.MaxInt, 0)
		if errVInisend != nil {
			log.Error(fmt.Sprintf("%s - Problem sending video init for delivery uni stream, err: %v", deliverySessionID, errVInisend))
			session.CloseWithError(1, "Problem sending video init")
			return
		}

		if audioJitterMs <= 0 {
			audioJitterMs = DELIVERY_SESSION_WINDOW_DEFAULT_MS
			log.Info(fmt.Sprintf("%s - Defaulting the audio session window to %d ms", deliverySessionID, audioJitterMs))
		}
		if videoJitterMs <= 0 {
			videoJitterMs = DELIVERY_SESSION_WINDOW_DEFAULT_MS
			log.Info(fmt.Sprintf("%s - Defaulting the video session window to %d ms", deliverySessionID, videoJitterMs))
		}
		log.Info(fmt.Sprintf("%s - rewindMs: %d ms, videoJitterMs: %d ms, audioJitterMs: %d ms, startedAt: %v, endAt: %v", deliverySessionID, rewindMs, videoJitterMs, audioJitterMs, startedAt, endAt))

		var exitFunc int32 = 0
		for atomic.LoadInt32(&exitFunc) <= 0 {
			somethingSent := false

			// Sequence based on seqId
			audioFileToSend, errGetAudioFile := getSendFile(sessionType, assetID, "audio", rewindMs, memFiles, deliverySession, audioJitterMs, startedAt, endAt)
			if errGetAudioFile != nil {
				if errGetAudioFile.Error() == "EOS" {
					log.Info(fmt.Sprintf("%s - Audio, detected end of stream", deliverySessionID))
					atomic.AddInt32(&exitFunc, 1)
				} else {
					log.Error(fmt.Sprintf("%s - Problem getting audio segment for delivery uni stream, err: %v", deliverySessionID, errGetAudioFile))
					session.CloseWithError(1, "Problem getting audio segment")
					return
				}
			}

			if audioFileToSend != nil {
				go func(f *memfile.MemFile) {
					sendPriority := getSendPriority(sessionType, f.Headers.MediaType, f.Headers.SeqId)
					expiration := getExpirationDuration(sessionType, audioJitterMs)

					errASend := sendFile(session, deliverySessionID, &inFlightReq, f, packagerVersion, sendPriority, expiration)
					if errASend != nil {
						log.Error(fmt.Sprintf("%s - Sending audio segment. SeqID: %d. Err: %v", deliverySessionID, f.Headers.SeqId, errASend))
						atomic.AddInt32(&exitFunc, 1) // exit Probably context is gone
					}
				}(audioFileToSend)
				somethingSent = true
			}

			if !somethingSent && atomic.LoadInt32(&inFlightReq) < MAX_INFLIGHT_REQUEST_BEFORE_STOP_VIDEO {
				videoFileToSend, errGetVideoFile := getSendFile(sessionType, assetID, "video", rewindMs, memFiles, deliverySession, videoJitterMs, startedAt, endAt)
				if errGetVideoFile != nil {
					if errGetVideoFile.Error() == "EOS" {
						log.Info(fmt.Sprintf("%s - Video, detected end of stream", deliverySessionID))
						atomic.AddInt32(&exitFunc, 1)
					} else {
						log.Error(fmt.Sprintf("%s - Problem getting video segment for delivery uni stream, err: %v", deliverySessionID, errGetVideoFile))
						session.CloseWithError(1, "Problem getting video segment")
						return
					}
				}
				if videoFileToSend != nil {
					go func(f *memfile.MemFile) {
						sendPriority := getSendPriority(sessionType, f.Headers.MediaType, f.Headers.SeqId)
						expiration := getExpirationDuration(sessionType, videoJitterMs)

						errVsend := sendFile(session, deliverySessionID, &inFlightReq, f, packagerVersion, sendPriority, expiration)
						if errVsend != nil {
							log.Error(fmt.Sprintf("%s - Sending video segment. SeqID: %d. Err: %v", deliverySessionID, f.Headers.SeqId, errVsend))
							atomic.AddInt32(&exitFunc, 1) // exit Probably context is gone
						}
					}(videoFileToSend)
					somethingSent = true
				}
			}

			if !somethingSent {
				time.Sleep(time.Duration(NO_MORE_FRAMES_WAIT_MS) * time.Millisecond)
			} else {
				log.Info(fmt.Sprintf("%s - Delivery sessions elements: %d", deliverySessionID, deliverySession.GetNumElements()))

				if atomic.LoadInt32(&inFlightReq) >= MAX_INFLIGHT_REQUEST {
					atomic.AddInt32(&exitFunc, 1)
					log.Error(fmt.Sprintf("%s - killing session because too many inflight requests", deliverySessionID))
				}
			}
		}
	}()
}

// Helpers

func getSendPriority(sessionType string, mediaType string, seqId int64) int {
	//TODO: To properly prioritize between sync streams we should use DTS
	// Consider rollovers
	var ret int = 0
	switch sessionType {
	case DeliverySessionLiveEdge:
		if mediaType == "audio" {
			ret = int(seqId) + AUDIO_OFFSET_SEND_PRIORITY
		} else {
			ret = int(seqId)
		}
	case DeliverySessionLiveRewind, DeliverySessionVOD:
		if mediaType == "audio" {
			ret = math.MaxInt - int(seqId)
		} else {
			ret = math.MaxInt - int(seqId) - AUDIO_OFFSET_SEND_PRIORITY
		}
	default:
		ret = 0
	}
	return ret
}

func getExpirationDuration(sessionType string, jitterBufferMs uint) time.Duration {
	var ret time.Duration = 0
	if sessionType == DeliverySessionLiveEdge && LIVE_CANCEL_AFTER_TIMES_JITTER > 0 {
		ret = time.Duration(jitterBufferMs*LIVE_CANCEL_AFTER_TIMES_JITTER) * time.Millisecond
	}
	return ret
}

func getDeliverySessionType(rewindMs uint, startedAt time.Time, endAt time.Time) string {
	if !startedAt.IsZero() && !endAt.IsZero() {
		// Vod / highlight
		return DeliverySessionVOD
	} else {
		if rewindMs > 0 {
			// Rewind
			return DeliverySessionLiveRewind
		}
	}
	// Edge
	return DeliverySessionLiveEdge
}

func sendFile(session *webtransport.Session, deliverySessionID string, inFlightReq *int32, f *memfile.MemFile, packagerVersion mediapackager.PackagerVersion, sendPriority int, expiration time.Duration) error {
	sUni, errOpenStream := session.OpenUniStreamSync(session.Context())
	if errOpenStream != nil {
		return errOpenStream
	}
	sUni.SetPriority(sendPriority)

	if expiration > 0 {
		// This closes gracefully the stream, so the received does NOT know it is uncomplete
		sUni.SetWriteDeadline(time.Now().Add(expiration))
	}

	atomic.AddInt32(inFlightReq, 1)

	atomic.LoadInt32(inFlightReq)

	log.Info(fmt.Sprintf("%s(%v) - Start sending frame. MediaType: %s, SeqID: %d (current inflight: %d)", deliverySessionID, sUni.StreamID(), f.Headers.MediaType, f.Headers.SeqId, atomic.LoadInt32(inFlightReq)))

	dataHeaderBytes, errDataHeaderEncode := mediapackager.Encode(f.Headers, packagerVersion)
	if errDataHeaderEncode != nil {
		atomic.AddInt32(inFlightReq, -1)
		return errors.New(fmt.Sprintf("Encoding header for streamID: %v, version: %s, err: %v", sUni.StreamID(), mediapackager.VersionToString(packagerVersion), errDataHeaderEncode))
	}

	dataHeaderLengthBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(dataHeaderLengthBytes, uint64(len(dataHeaderBytes)))
	sUni.Write(dataHeaderLengthBytes)
	sUni.Write(dataHeaderBytes)

	dataBlock := make([]byte, COPY_BLOCK_BYTES)
	srcReader := f.NewReadCloser()
	readBytes := 0
	totalSent := 0
	var errRead error = nil
	for errRead == nil {
		readBytes, errRead = srcReader.Read(dataBlock)
		if readBytes > 0 {
			sUni.Write(dataBlock[:readBytes])
			totalSent += readBytes
		}
	}
	errReaderClose := srcReader.Close()
	if errReaderClose != nil {
		atomic.AddInt32(inFlightReq, -1)
		return errReaderClose
	}
	errSend := sUni.Close()
	if errSend == nil {
		log.Info(fmt.Sprintf("%s(%v) - Sent frame. MediaType: %s, SeqID: %d", deliverySessionID, sUni.StreamID(), f.Headers.MediaType, f.Headers.SeqId))
	}

	atomic.AddInt32(inFlightReq, -1)

	return errSend
}

func getSendFile(sessionType string, assetID string, mediaType string, rewindMs uint, memFiles *memfiles.MemFiles, deliverySession *deliverysession.DeliverySession, playerJitterBufferMs uint, startedAt time.Time, endAt time.Time) (f *memfile.MemFile, err error) {
	switch sessionType {
	case DeliverySessionLiveEdge:
		f = memFiles.GetFileForAssetNewestSeqId(assetID, mediaType, time.Duration(playerJitterBufferMs)*time.Millisecond, deliverySession)
	case DeliverySessionLiveRewind:
		f = memFiles.GetNextByTimeSeqId(assetID, mediaType, time.Duration(playerJitterBufferMs)*time.Millisecond, time.Duration(rewindMs)*time.Millisecond, deliverySession)
	case DeliverySessionVOD:
		f, err = memFiles.GetNextByStartEnd(assetID, mediaType, time.Duration(playerJitterBufferMs)*time.Millisecond, startedAt, endAt, deliverySession)
	default:
		err = errors.New(fmt.Sprintf("Unknown session type: %s", sessionType))
	}

	return
}

func parseWTQSData(urlQS url.Values) (bufferSizeMs uint, videoJitterMs uint, audioJitterMs uint, startedAt time.Time, endAt time.Time, packagerVersion mediapackager.PackagerVersion) {
	// Get buffer size (ms)
	bufferSizeSecsMsStr := urlQS.Get("old_ms")
	if bufferSizeSecsMsStr != "" {
		bufferSizeMsTmp, errConv := strconv.Atoi(bufferSizeSecsMsStr)
		if errConv == nil && bufferSizeMsTmp > 0 {
			bufferSizeMs = uint(bufferSizeMsTmp)
		}
	}
	videoJitterMsTmp, errVj := strconv.Atoi(urlQS.Get("vj_ms"))
	if errVj == nil {
		videoJitterMs = uint(videoJitterMsTmp)
	}

	audioJitterMsTmp, errAj := strconv.Atoi(urlQS.Get("aj_ms"))
	if errAj == nil {
		audioJitterMs = uint(audioJitterMsTmp)
	}

	startedAtEpochMs, errSa := strconv.ParseInt(urlQS.Get("sa"), 10, 64)
	if errSa == nil {
		startedAt = time.UnixMilli(startedAtEpochMs)
	}

	endAtEpochMs, errSa := strconv.ParseInt(urlQS.Get("ea"), 10, 64)
	if errSa == nil {
		endAt = time.UnixMilli(endAtEpochMs)
	}

	packagerVersion = mediapackager.StringToVersion(urlQS.Get("pk"))

	return
}

func getAssetInfo(header memfile.FileHeader) (mediaType string, seqId int64, isInit bool) {
	seqId = header.SeqId
	mediaType = header.MediaType
	if seqId < 0 {
		isInit = true
	}
	return
}

// Main function

func main() {
	log.SetFormatter(&log.TextFormatter{})

	// create memfiles
	memFiles := memfiles.New(CACHE_CLEAN_UP_PERIOD_MS)

	// Note: "new-tab-page" in AllowedOrigins lets you access the server from a blank tab (via DevTools Console).
	// "" in AllowedOrigins lets you access the server from JavaScript loaded from disk (i.e. via a file:// URL)
	server := &webtransport.Server{
		H3:          http3.Server{Addr: ":4433"},
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	http.HandleFunc("/moqingest/", func(rw http.ResponseWriter, r *http.Request) {
		ingestSessionID := "I-" + uuid.New().String() + "-" + r.URL.Path

		conn, err := server.Upgrade(rw, r)
		if err != nil {
			log.Printf("upgrading failed: %s", err)
			log.Error(fmt.Sprintf("%s - Ingest connection upgrade failed, rawQuery: %s", ingestSessionID, r.URL.RawQuery))

			rw.WriteHeader(500)
			return
		}
		log.Info(fmt.Sprintf("%s - Accepted incoming WebTransport session. rawQuery: %s", ingestSessionID, r.URL.RawQuery))

		handleWebTransportIngestStreams(conn, ingestSessionID, r.URL.Path, r.URL.Query(), memFiles)
	})

	http.HandleFunc("/moqdelivery/", func(rw http.ResponseWriter, r *http.Request) {
		deliverySessionID := "D-" + uuid.New().String() + "-" + r.URL.Path
		conn, err := server.Upgrade(rw, r)
		if err != nil {
			log.Error(fmt.Sprintf("%s - Delivery connection upgrade failed, rawQuery: %s", deliverySessionID, r.URL.RawQuery))

			rw.WriteHeader(500)
			return
		}
		log.Info(fmt.Sprintf("%s - Accepted incoming WebTransport session. rawQuery: %s", deliverySessionID, r.URL.RawQuery))

		handleWebTransportDeliveryStreams(conn, deliverySessionID, r.URL.Path, r.URL.Query(), memFiles)
	})

	log.Info("Launching WebTransport server at: ", server.H3.Addr)

	server.ListenAndServeTLS("../certs/certificate.pem", "../certs/certificate.key")

	memFiles.Stop()
}
