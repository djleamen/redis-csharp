using System.Text;

namespace codecrafters_redis;

partial class RedisServer
{
    /// <summary>
    /// Adds a geospatial member to the sorted set at the key specified in <paramref name="parts"/>,
    /// encoding the longitude/latitude as a geohash score.
    /// </summary>
    private string GeoAdd(string[] parts)
    {
        if (!double.TryParse(parts[2], System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out double lon) ||
            !double.TryParse(parts[3], System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out double lat))
        {
            return "-ERR invalid longitude,latitude pair\r\n";
        }

        if (lon < -180.0 || lon > 180.0)
            return $"-ERR invalid longitude value {lon:F6}\r\n";

        if (lat < -85.05112878 || lat > 85.05112878)
            return $"-ERR invalid latitude value {lat:F6}\r\n";

        string key = parts[1];
        string member = parts[4];
        double score = GeoUtils.EncodeGeoHash(lon, lat);

        if (!_dataStore.ContainsKey(key))
        {
            _dataStore[key] = new StoredValue(new List<SortedSetEntry> { new(member, score) });
            return ":1\r\n";
        }

        if (!_dataStore.TryGetValue(key, out StoredValue? sv) || sv.SortedSet == null)
            return WrongTypeError;

        var existing = sv.SortedSet.FirstOrDefault(e => e.Member == member);
        if (existing != null)
        {
            sv.SortedSet.Remove(existing);
            sv.SortedSet.Add(new SortedSetEntry(member, score));
            sv.SortedSet.Sort();
            return ":0\r\n";
        }

        sv.SortedSet.Add(new SortedSetEntry(member, score));
        sv.SortedSet.Sort();
        return ":1\r\n";
    }

    /// <summary>
    /// Returns the longitude and latitude of one or more members stored in the
    /// geo sorted set identified by the key in <paramref name="parts"/>.
    /// </summary>
    private string GeoPos(string[] parts)
    {
        string key = parts[1];
        int memberCount = parts.Length - 2;
        var sb = new StringBuilder();
        sb.Append($"*{memberCount}\r\n");

        for (int i = 2; i < parts.Length; i++)
        {
            string member = parts[i];
            if (_dataStore.TryGetValue(key, out StoredValue? sv) && sv.SortedSet != null)
            {
                var entry = sv.SortedSet.FirstOrDefault(e => e.Member == member);
                if (entry != null)
                {
                    var (decLon, decLat) = GeoUtils.DecodeGeoHash((long)entry.Score);
                    string lonStr = decLon.ToString("R", System.Globalization.CultureInfo.InvariantCulture);
                    string latStr = decLat.ToString("R", System.Globalization.CultureInfo.InvariantCulture);
                    sb.Append($"*2\r\n${Encoding.UTF8.GetByteCount(lonStr)}\r\n{lonStr}\r\n${Encoding.UTF8.GetByteCount(latStr)}\r\n{latStr}\r\n");
                    continue;
                }
            }
            sb.Append("*-1\r\n");
        }

        return sb.ToString();
    }

    /// <summary>
    /// Computes the great-circle distance in meters between two members of the geo
    /// sorted set at <paramref name="key"/>.
    /// </summary>
    private string GeoDist(string key, string member1, string member2)
    {
        if (!_dataStore.TryGetValue(key, out StoredValue? sv) || sv.SortedSet == null)
            return "$-1\r\n";

        var e1 = sv.SortedSet.FirstOrDefault(e => e.Member == member1);
        var e2 = sv.SortedSet.FirstOrDefault(e => e.Member == member2);
        if (e1 == null || e2 == null)
            return "$-1\r\n";

        var (lon1, lat1) = GeoUtils.DecodeGeoHash((long)e1.Score);
        var (lon2, lat2) = GeoUtils.DecodeGeoHash((long)e2.Score);
        double dist = GeoUtils.DistanceMeters(lat1, lon1, lat2, lon2);
        string distStr = dist.ToString("F4", System.Globalization.CultureInfo.InvariantCulture);
        return $"${Encoding.UTF8.GetByteCount(distStr)}\r\n{distStr}\r\n";
    }

    /// <summary>
    /// Searches for members within a radius of a given longitude/latitude in the geo
    /// sorted set. Currently supports only FROMLONLAT + BYRADIUS.
    /// </summary>
    private string GeoSearch(string[] parts)
    {
        string key = parts[1];

        if (parts[2].ToUpper() != "FROMLONLAT" || parts[5].ToUpper() != "BYRADIUS")
            return "-ERR unsupported GEOSEARCH options\r\n";

        if (!double.TryParse(parts[3], System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out double centerLon) ||
            !double.TryParse(parts[4], System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out double centerLat) ||
            !double.TryParse(parts[6], System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out double radius))
        {
            return "-ERR invalid arguments\r\n";
        }

        double unitMultiplier = parts[7].ToLower() switch
        {
            "km" => 1000.0,
            "mi" => 1609.344,
            "ft" => 0.3048,
            _ => 1.0
        };
        double radiusMeters = radius * unitMultiplier;

        var matches = new List<string>();
        if (_dataStore.TryGetValue(key, out StoredValue? sv) && sv.SortedSet != null)
        {
            foreach (var entry in sv.SortedSet)
            {
                var (mLon, mLat) = GeoUtils.DecodeGeoHash((long)entry.Score);
                if (GeoUtils.DistanceMeters(centerLat, centerLon, mLat, mLon) <= radiusMeters)
                    matches.Add(entry.Member);
            }
        }

        var sb = new StringBuilder();
        sb.Append($"*{matches.Count}\r\n");
        foreach (var m in matches)
            sb.Append($"${Encoding.UTF8.GetByteCount(m)}\r\n{m}\r\n");
        return sb.ToString();
    }
}
