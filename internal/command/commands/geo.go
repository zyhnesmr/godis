// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package commands

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/zyhnesmr/godis/internal/command"
	"github.com/zyhnesmr/godis/internal/database"
	geopkg "github.com/zyhnesmr/godis/internal/datastruct/geo"
	"github.com/zyhnesmr/godis/internal/datastruct/zset"
)

// RegisterGeoCommands registers all geo commands
func RegisterGeoCommands(disp Dispatcher) {
	disp.Register(&command.Command{
		Name:       "GEOADD",
		Handler:    geoaddCmd,
		Arity:      -5,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatGeo},
	})

	disp.Register(&command.Command{
		Name:       "GEODIST",
		Handler:    geodistCmd,
		Arity:      -4,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatGeo},
	})

	disp.Register(&command.Command{
		Name:       "GEOHASH",
		Handler:    geohashCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatGeo},
	})

	disp.Register(&command.Command{
		Name:       "GEOPOS",
		Handler:    geoposCmd,
		Arity:      -2,
		Flags:      []string{command.FlagReadOnly},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatGeo},
	})

	disp.Register(&command.Command{
		Name:       "GEORADIUS",
		Handler:    georadiusCmd,
		Arity:      -5,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatGeo},
	})

	disp.Register(&command.Command{
		Name:       "GEORADIUSBYMEMBER",
		Handler:    georadiusbymemberCmd,
		Arity:      -4,
		Flags:      []string{command.FlagWrite, command.FlagDenyOOM},
		FirstKey:   1,
		LastKey:    1,
		Categories: []string{command.CatGeo},
	})
}

// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
func geoaddCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 4 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	args := ctx.Args[1:]

	// Parse options
	nx := false
	xx := false
	ch := false

	for len(args) > 0 && (args[0] == "NX" || args[0] == "XX" || args[0] == "CH") {
		opt := strings.ToUpper(args[0])
		if opt == "NX" {
			nx = true
		} else if opt == "XX" {
			xx = true
		} else if opt == "CH" {
			ch = true
		}
		args = args[1:]
	}

	if nx && xx {
		return nil, errors.New("NX and XX options at the same time")
	}

	// Remaining args must be in groups of 3: longitude latitude member
	if len(args)%3 != 0 {
		return nil, errors.New("wrong number of arguments")
	}

	// Get or create ZSet
	var zs *zset.ZSet
	obj, ok := ctx.DB.Get(key)
	if ok {
		if obj.Type != database.ObjTypeZSet {
			return nil, errors.New("WRONGTYPE Key is not a valid geoset")
		}
		zs = obj.Ptr.(*zset.ZSet)
	} else {
		zs = zset.NewZSet()
	}

	added := 0
	updated := 0

	// Process each triple: longitude latitude member
	for i := 0; i < len(args); i += 3 {
		longitude, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			return nil, errors.New("invalid longitude")
		}

		latitude, err := strconv.ParseFloat(args[i+1], 64)
		if err != nil {
			return nil, errors.New("invalid latitude")
		}

		member := args[i+2]

		// Validate coordinates
		if !geopkg.ValidateCoordinates(longitude, latitude) {
			return nil, errors.New("invalid coordinates")
		}

		// Encode to score
		score := geopkg.EncodeToScore(longitude, latitude)

		// Check if member exists
		if _, found := zs.Score(member); found {
			if nx {
				continue // Skip - NX means only add new elements
			}
			// Update existing
			oldScore, _ := zs.Score(member)
			if !ch || oldScore != score {
				zs.Remove(member)
				zs.Add(member, score)
				updated++
			}
		} else {
			if xx {
				continue // Skip - XX means only update existing
			}
			zs.Add(member, score)
			added++
		}
	}

	// Store back
	obj = database.NewObject(database.ObjTypeZSet, database.ObjEncodingSkiplist, zs)
	ctx.DB.Set(key, obj)

	// Return number of elements added (or updated if CH is set)
	if ch {
		return command.NewIntegerReply(int64(added + updated)), nil
	}
	return command.NewIntegerReply(int64(added)), nil
}

// GEODIST key member1 member2 [unit]
func geodistCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 3 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	member1 := ctx.Args[1]
	member2 := ctx.Args[2]

	unit := geopkg.Meters
	if len(ctx.Args) >= 4 {
		switch strings.ToUpper(ctx.Args[3]) {
		case "M", "METERS", "METRE", "METRES":
			unit = geopkg.Meters
		case "KM", "KILOMETERS", "KILOMETRE", "KILOMETRES":
			unit = geopkg.Kilometers
		case "MI", "MILE", "MILES":
			unit = geopkg.Miles
		case "FT", "FOOT", "FEET":
			unit = geopkg.Feet
		default:
			return nil, errors.New("unknown unit")
		}
	}

	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewNilReply(), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("WRONGTYPE Key is not a valid geoset")
	}

	zs := obj.Ptr.(*zset.ZSet)

	// Get scores for both members
	score1, ok1 := zs.Score(member1)
	score2, ok2 := zs.Score(member2)

	if !ok1 || !ok2 {
		return command.NewNilReply(), nil
	}

	// Decode scores to coordinates
	lon1, lat1 := geopkg.DecodeFromScore(score1)
	lon2, lat2 := geopkg.DecodeFromScore(score2)

	p1 := &geopkg.Point{Longitude: lon1, Latitude: lat1}
	p2 := &geopkg.Point{Longitude: lon2, Latitude: lat2}

	distance := geopkg.GetDistance(p1, p2)
	result := geopkg.FromMeters(distance, unit)

	return command.NewBulkStringReply(fmt.Sprintf("%.4f", result)), nil
}

// GEOHASH key member [member ...]
func geohashCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	members := ctx.Args[1:]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		// Return nil for all members
		results := make([]string, len(members))
		return command.NewStringArrayReply(results), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("WRONGTYPE Key is not a valid geoset")
	}

	zs := obj.Ptr.(*zset.ZSet)

	results := make([]string, len(members))
	for i, member := range members {
		if score, ok := zs.Score(member); ok {
			lon, lat := geopkg.DecodeFromScore(score)
			results[i] = geopkg.EncodeToBase32(lon, lat, 10)
		}
		// else: results[i] remains "" (nil in RESP)
	}

	return command.NewStringArrayReply(results), nil
}

// GEOPOS key member [member ...]
func geoposCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 2 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	members := ctx.Args[1:]

	obj, ok := ctx.DB.Get(key)
	if !ok {
		// Return nil for all members
		results := make([]interface{}, len(members))
		return command.NewArrayReplyFromAny(results), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("WRONGTYPE Key is not a valid geoset")
	}

	zs := obj.Ptr.(*zset.ZSet)

	results := make([]interface{}, len(members))
	for i, member := range members {
		if score, ok := zs.Score(member); ok {
			lon, lat := geopkg.DecodeFromScore(score)
			// Return array of [longitude, latitude]
			results[i] = []interface{}{lon, lat}
		}
		// else: results[i] remains nil
	}

	return command.NewArrayReplyFromAny(results), nil
}

// GEORADIUS key longitude latitude radius unit [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count]
func georadiusCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 5 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	longitude, err := strconv.ParseFloat(ctx.Args[1], 64)
	if err != nil {
		return nil, errors.New("invalid longitude")
	}

	latitude, err := strconv.ParseFloat(ctx.Args[2], 64)
	if err != nil {
		return nil, errors.New("invalid latitude")
	}

	radius, err := strconv.ParseFloat(ctx.Args[3], 64)
	if err != nil || radius < 0 {
		return nil, errors.New("invalid radius")
	}

	var unit geopkg.DistanceUnit
	switch strings.ToUpper(ctx.Args[4]) {
	case "M", "METERS", "METRE", "METRES":
		unit = geopkg.Meters
	case "KM", "KILOMETERS", "KILOMETRE", "KILOMETRES":
		unit = geopkg.Kilometers
	case "MI", "MILE", "MILES":
		unit = geopkg.Miles
	case "FT", "FOOT", "FEET":
		unit = geopkg.Feet
	default:
		return nil, errors.New("unknown unit")
	}

	// Convert radius to meters
	radiusMeters := geopkg.ToMeters(radius, unit)

	// Parse options
	args := ctx.Args[5:]
	withCoord := false
	withDist := false
	withHash := false
	count := 0
	countSet := false
	asc := true
	storeKey := ""
	storeDistKey := ""

	for i := 0; i < len(args); i++ {
		opt := strings.ToUpper(args[i])
		switch opt {
		case "WITHCOORD":
			withCoord = true
		case "WITHDIST":
			withDist = true
		case "WITHHASH":
			withHash = true
		case "COUNT":
			if i+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			count, err = strconv.Atoi(args[i+1])
			if err != nil || count < 0 {
				return nil, errors.New("invalid count")
			}
			countSet = true
			i++
		case "ASC":
			asc = true
		case "DESC":
			asc = false
		case "STORE":
			if i+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			storeKey = args[i+1]
			i++
		case "STOREDIST":
			if i+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			storeDistKey = args[i+1]
			i++
		}
	}

	// Get ZSet
	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewArrayReplyFromAny([]interface{}{}), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("WRONGTYPE Key is not a valid geoset")
	}

	zs := obj.Ptr.(*zset.ZSet)

	// Get all members and calculate distances
	center := &geopkg.Point{Longitude: longitude, Latitude: latitude}
	type result struct {
		member string
		score  float64
		lon    float64
		lat    float64
		dist   float64
		hash   uint64
	}

	var results []result
	members := zs.Members()

	for _, member := range members {
		score, _ := zs.Score(member)
		lon, lat := geopkg.DecodeFromScore(score)
		point := &geopkg.Point{Longitude: lon, Latitude: lat}
		dist := geopkg.GetDistance(center, point)

		if dist <= radiusMeters {
			results = append(results, result{
				member: member,
				score:  score,
				lon:    lon,
				lat:    lat,
				dist:   dist,
				hash:   uint64(score),
			})
		}
	}

	// Sort by distance
	sort.Slice(results, func(i, j int) bool {
		if asc {
			return results[i].dist < results[j].dist
		}
		return results[i].dist > results[j].dist
	})

	// Apply count limit
	if countSet && count > 0 && count < len(results) {
		results = results[:count]
	}

	// Handle STORE options
	if storeKey != "" {
		storeZs := zset.NewZSet()
		for _, r := range results {
			storeZs.Add(r.member, r.score)
		}
		storeObj := database.NewObject(database.ObjTypeZSet, database.ObjEncodingSkiplist, storeZs)
		ctx.DB.Set(storeKey, storeObj)
		return command.NewIntegerReply(int64(len(results))), nil
	}

	if storeDistKey != "" {
		storeZs := zset.NewZSet()
		for _, r := range results {
			// Store distance as score (converted to meters for consistency)
			storeZs.Add(r.member, r.dist)
		}
		storeObj := database.NewObject(database.ObjTypeZSet, database.ObjEncodingSkiplist, storeZs)
		ctx.DB.Set(storeDistKey, storeObj)
		return command.NewIntegerReply(int64(len(results))), nil
	}

	// Build reply
	reply := make([]interface{}, len(results))
	for i, r := range results {
		items := []interface{}{r.member}

		if withDist {
			items = append(items, fmt.Sprintf("%.4f", geopkg.FromMeters(r.dist, unit)))
		}
		if withHash {
			items = append(items, fmt.Sprintf("%d", r.hash))
		}
		if withCoord {
			items = append(items, []interface{}{r.lon, r.lat})
		}

		if len(items) == 1 {
			reply[i] = r.member
		} else {
			reply[i] = items
		}
	}

	return command.NewArrayReplyFromAny(reply), nil
}

// GEORADIUSBYMEMBER key member radius unit [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count]
func georadiusbymemberCmd(ctx *command.Context) (*command.Reply, error) {
	if len(ctx.Args) < 4 {
		return nil, errors.New("wrong number of arguments")
	}

	key := ctx.Args[0]
	member := ctx.Args[1]
	radius, err := strconv.ParseFloat(ctx.Args[2], 64)
	if err != nil || radius < 0 {
		return nil, errors.New("invalid radius")
	}

	var unit geopkg.DistanceUnit
	switch strings.ToUpper(ctx.Args[3]) {
	case "M", "METERS", "METRE", "METRES":
		unit = geopkg.Meters
	case "KM", "KILOMETERS", "KILOMETRE", "KILOMETRES":
		unit = geopkg.Kilometers
	case "MI", "MILE", "MILES":
		unit = geopkg.Miles
	case "FT", "FOOT", "FEET":
		unit = geopkg.Feet
	default:
		return nil, errors.New("unknown unit")
	}

	radiusMeters := geopkg.ToMeters(radius, unit)

	// Parse options
	args := ctx.Args[4:]
	withCoord := false
	withDist := false
	withHash := false
	count := 0
	countSet := false
	asc := true
	storeKey := ""
	storeDistKey := ""

	for i := 0; i < len(args); i++ {
		opt := strings.ToUpper(args[i])
		switch opt {
		case "WITHCOORD":
			withCoord = true
		case "WITHDIST":
			withDist = true
		case "WITHHASH":
			withHash = true
		case "COUNT":
			if i+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			count, err = strconv.Atoi(args[i+1])
			if err != nil || count < 0 {
				return nil, errors.New("invalid count")
			}
			countSet = true
			i++
		case "ASC":
			asc = true
		case "DESC":
			asc = false
		case "STORE":
			if i+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			storeKey = args[i+1]
			i++
		case "STOREDIST":
			if i+1 >= len(args) {
				return nil, errors.New("syntax error")
			}
			storeDistKey = args[i+1]
			i++
		}
	}

	// Get ZSet
	obj, ok := ctx.DB.Get(key)
	if !ok {
		return command.NewArrayReplyFromAny([]interface{}{}), nil
	}

	if obj.Type != database.ObjTypeZSet {
		return nil, errors.New("WRONGTYPE Key is not a valid geoset")
	}

	zs := obj.Ptr.(*zset.ZSet)

	// Get center member coordinates
	centerScore, ok := zs.Score(member)
	if !ok {
		return command.NewArrayReplyFromAny([]interface{}{}), nil
	}

	centerLon, centerLat := geopkg.DecodeFromScore(centerScore)
	center := &geopkg.Point{Longitude: centerLon, Latitude: centerLat}

	type result struct {
		member string
		score  float64
		lon    float64
		lat    float64
		dist   float64
		hash   uint64
	}

	var results []result
	members := zs.Members()

	for _, m := range members {
		if m == member {
			continue // Skip the center member itself
		}
		score, _ := zs.Score(m)
		lon, lat := geopkg.DecodeFromScore(score)
		point := &geopkg.Point{Longitude: lon, Latitude: lat}
		dist := geopkg.GetDistance(center, point)

		if dist <= radiusMeters {
			results = append(results, result{
				member: m,
				score:  score,
				lon:    lon,
				lat:    lat,
				dist:   dist,
				hash:   uint64(score),
			})
		}
	}

	// Sort by distance
	sort.Slice(results, func(i, j int) bool {
		if asc {
			return results[i].dist < results[j].dist
		}
		return results[i].dist > results[j].dist
	})

	// Apply count limit
	if countSet && count > 0 && count < len(results) {
		results = results[:count]
	}

	// Handle STORE options
	if storeKey != "" {
		storeZs := zset.NewZSet()
		for _, r := range results {
			storeZs.Add(r.member, r.score)
		}
		storeObj := database.NewObject(database.ObjTypeZSet, database.ObjEncodingSkiplist, storeZs)
		ctx.DB.Set(storeKey, storeObj)
		return command.NewIntegerReply(int64(len(results))), nil
	}

	if storeDistKey != "" {
		storeZs := zset.NewZSet()
		for _, r := range results {
			storeZs.Add(r.member, r.dist)
		}
		storeObj := database.NewObject(database.ObjTypeZSet, database.ObjEncodingSkiplist, storeZs)
		ctx.DB.Set(storeDistKey, storeObj)
		return command.NewIntegerReply(int64(len(results))), nil
	}

	// Build reply
	reply := make([]interface{}, len(results))
	for i, r := range results {
		items := []interface{}{r.member}

		if withDist {
			items = append(items, fmt.Sprintf("%.4f", geopkg.FromMeters(r.dist, unit)))
		}
		if withHash {
			items = append(items, fmt.Sprintf("%d", r.hash))
		}
		if withCoord {
			items = append(items, []interface{}{r.lon, r.lat})
		}

		if len(items) == 1 {
			reply[i] = r.member
		} else {
			reply[i] = items
		}
	}

	return command.NewArrayReplyFromAny(reply), nil
}
