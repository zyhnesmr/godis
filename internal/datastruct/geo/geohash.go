// Copyright 2024 The Godis Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package geo

import (
	"math"
	"strings"
)

const (
	// Earth radius in meters
	EarthRadius = 6372797.560856
	// Miles per meter
	MilesPerMeter = 0.000621371
	// Feet per meter
	FeetPerMeter = 3.28084
	// Longitude range
	MinLongitude = -180
	MaxLongitude = 180
	// Latitude range
	MinLatitude = -85.05112878
	MaxLatitude = 85.05112878
	// Bits for geohash encoding
	GeoHashBits = 52
)

// Point represents a geographic coordinate
type Point struct {
	Longitude float64
	Latitude  float64
	Dist      float64 // Distance (for sorting)
	Hash      string  // Geohash string
	Score     float64 // ZSet score
}

// GeoHash represents a geohash value with score
type GeoHash struct {
	Score  float64
	Point  *Point
	Member string
}

// DistanceUnit represents the unit for distance calculation
type DistanceUnit int

const (
	Meters DistanceUnit = iota
	Kilometers
	Miles
	Feet
)

// ToMeters converts distance to meters based on unit
func ToMeters(dist float64, unit DistanceUnit) float64 {
	switch unit {
	case Kilometers:
		return dist * 1000
	case Miles:
		return dist / MilesPerMeter
	case Feet:
		return dist / FeetPerMeter
	default: // Meters
		return dist
	}
}

// FromMeters converts meters to specified unit
func FromMeters(meters float64, unit DistanceUnit) float64 {
	switch unit {
	case Kilometers:
		return meters / 1000
	case Miles:
		return meters * MilesPerMeter
	case Feet:
		return meters * FeetPerMeter
	default: // Meters
		return meters
	}
}

// EncodeToScore encodes a longitude and latitude to a 52-bit score for ZSet
// This matches Redis's geohash encoding
func EncodeToScore(longitude, latitude float64) float64 {
	// Normalize to 0-1 range
	lonNorm := (longitude - MinLongitude) / (MaxLongitude - MinLongitude)
	latNorm := (latitude - MinLatitude) / (MaxLatitude - MinLatitude)

	// Interleave bits: 52 bits total, 26 for each dimension
	var score uint64

	// We use 26 bits for precision
	for i := 0; i < 26; i++ {
		// Get the i-th bit from the high end of normalized values
		// lonBit is bit i of the 26-bit longitude value
		lonVal := uint64(lonNorm * float64(1<<26))
		lonBit := (lonVal >> (25 - i)) & 1

		// latBit is bit i of the 26-bit latitude value
		latVal := uint64(latNorm * float64(1<<26))
		latBit := (latVal >> (25 - i)) & 1

		// Interleave: even bits are longitude, odd bits are latitude
		score = (score << 1) | lonBit
		score = (score << 1) | latBit
	}

	return float64(score)
}

// DecodeFromScore decodes a score back to longitude and latitude
func DecodeFromScore(score float64) (longitude, latitude float64) {
	bits := uint64(score)

	var lonBits uint64
	var latBits uint64

	// De-interleave bits
	for i := 0; i < 26; i++ {
		lonBit := (bits >> (51 - 2*i)) & 1
		latBit := (bits >> (50 - 2*i)) & 1

		lonBits = (lonBits << 1) | lonBit
		latBits = (latBits << 1) | latBit
	}

	// Convert back to normalized values
	lonNorm := float64(lonBits) / float64(1<<26)
	latNorm := float64(latBits) / float64(1<<26)

	// Convert back to actual coordinates
	longitude = MinLongitude + lonNorm*(MaxLongitude-MinLongitude)
	latitude = MinLatitude + latNorm*(MaxLatitude-MinLatitude)

	return longitude, latitude
}

// GetDistance calculates the distance between two points using Haversine formula
// Returns distance in meters
func GetDistance(p1, p2 *Point) float64 {
	// Convert to radians
	lat1 := toRadians(p1.Latitude)
	lat2 := toRadians(p2.Latitude)
	lon1 := toRadians(p1.Longitude)
	lon2 := toRadians(p2.Longitude)

	dLat := lat2 - lat1
	dLon := lon2 - lon1

	// Haversine formula
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*
			math.Sin(dLon/2)*math.Sin(dLon/2)

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return EarthRadius * c
}

// toRadians converts degrees to radians
func toRadians(degrees float64) float64 {
	return degrees * math.Pi / 180
}

// toDegrees converts radians to degrees
func toDegrees(radians float64) float64 {
	return radians * 180 / math.Pi
}

// EncodeToBase32 encodes coordinates to a geohash string (base32)
func EncodeToBase32(longitude, latitude float64, precision int) string {
	const base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

	// Standard geohash range
	lonRange := []float64{-180, 180}
	latRange := []float64{-90, 90}

	var bits uint8
	bitCount := 0
	var hash strings.Builder
	isEven := true

	for hash.Len() < precision {
		if isEven {
			mid := (lonRange[0] + lonRange[1]) / 2
			if longitude > mid {
				bits = (bits << 1) | 1
				lonRange[0] = mid
			} else {
				bits = bits << 1
				lonRange[1] = mid
			}
		} else {
			mid := (latRange[0] + latRange[1]) / 2
			if latitude > mid {
				bits = (bits << 1) | 1
				latRange[0] = mid
			} else {
				bits = bits << 1
				latRange[1] = mid
			}
		}
		isEven = !isEven
		bitCount++

		if bitCount == 5 {
			idx := bits & 0x1F // 5 bits
			hash.WriteByte(base32[idx])
			bits = 0
			bitCount = 0
		}
	}

	return hash.String()
}

// CalculateBoundingBox calculates a bounding box around a center point
func CalculateBoundingBox(center *Point, radiusMeters float64) (minLon, maxLon, minLat, maxLat float64) {
	// Convert radius to degrees (approximate)
	latDelta := (radiusMeters / 111320) * 180 / math.Pi
	lonDelta := (radiusMeters / (111320 * math.Cos(toRadians(center.Latitude)))) * 180 / math.Pi

	minLon = center.Longitude - lonDelta
	maxLon = center.Longitude + lonDelta
	minLat = center.Latitude - latDelta
	maxLat = center.Latitude + latDelta

	// Clamp to valid ranges
	minLon = math.Max(minLon, MinLongitude)
	maxLon = math.Min(maxLon, MaxLongitude)
	minLat = math.Max(minLat, MinLatitude)
	maxLat = math.Min(maxLat, MaxLatitude)

	return minLon, maxLon, minLat, maxLat
}

// SortByDistance sorts a slice of Points by distance from a center point
func SortByDistance(points []*Point, center *Point) {
	for _, p := range points {
		p.Dist = GetDistance(center, p)
	}

	// Simple bubble sort for small lists
	for i := 0; i < len(points); i++ {
		for j := i + 1; j < len(points); j++ {
			if points[i].Dist > points[j].Dist {
				points[i], points[j] = points[j], points[i]
			}
		}
	}
}

// ValidateCoordinates checks if the coordinates are valid
func ValidateCoordinates(longitude, latitude float64) bool {
	return longitude >= MinLongitude && longitude <= MaxLongitude &&
		latitude >= MinLatitude && latitude <= MaxLatitude
}
