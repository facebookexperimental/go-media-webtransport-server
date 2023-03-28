/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package memfile

import (
	"regexp"
	"strconv"
)

// Decoding headers
type FileHeader struct {
	CacheControl  string `json:"Cache-Control"`
	MediaType     string `json:"Joc-Media-Type"`
	Timestamp     int64  `json:"Joc-Timestamp"`
	Duration      int64  `json:"Joc-Duration"`
	ChunkType     string `json:"Joc-Chunk-Type"`
	SeqId         int64  `json:"Joc-Seq-Id"`
	FirstFrameClk int64  `json:"Joc-First-Frame-Clk"`
	UniqueId      string `json:"Joc-Uniq-Id"`
}

// Helper
func GetMaxAgeFromCacheControlOr(s string, def int64) int64 {
	ret := def
	r := regexp.MustCompile(`max-age=(?P<maxage>\d*)`)
	match := r.FindStringSubmatch(s)
	for i, name := range r.SubexpNames() {
		if i > 0 && i <= len(match) {
			if name == "maxage" {
				valInt, err := strconv.ParseInt(match[i], 10, 64)
				if err == nil {
					ret = valInt
					break
				}
			}
		}
	}
	return ret
}
