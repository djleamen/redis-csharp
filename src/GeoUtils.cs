namespace codecrafters_redis;

/// <summary>
/// Stateless geospatial utilities implementing the same geohash encoding and
/// distance calculations used by Redis.
/// </summary>
static class GeoUtils
{
    private const double EarthRadiusMeters = 6372797.560856;

    /// <summary>
    /// Encodes a longitude/latitude coordinate pair into a 52-bit interleaved geohash integer,
    /// matching Redis's <c>interleave64(lat, lon)</c> layout where latitude occupies even bit
    /// positions (0, 2, 4, …) and longitude occupies odd bit positions (1, 3, 5, …).
    /// </summary>
    /// <param name="longitude">Longitude in degrees in the range [-180, 180].</param>
    /// <param name="latitude">Latitude in degrees in the range [-85.05112878, 85.05112878].</param>
    /// <returns>A 52-bit geohash integer suitable for use as a sorted-set score.</returns>
    public static long EncodeGeoHash(double longitude, double latitude)
    {
        double normLon = (longitude + 180.0) / 360.0;
        double normLat = (latitude + 85.05112878) / 170.10225756;

        long lonBits = Math.Max(0L, Math.Min((1L << 26) - 1, (long)(normLon * (1L << 26))));
        long latBits = Math.Max(0L, Math.Min((1L << 26) - 1, (long)(normLat * (1L << 26))));

        long result = 0;
        for (int i = 0; i < 26; i++)
        {
            result |= ((latBits >> i) & 1L) << (2 * i);
            result |= ((lonBits >> i) & 1L) << (2 * i + 1);
        }
        return result;
    }

    /// <summary>
    /// Decodes a Redis geohash integer score back into a (longitude, latitude) coordinate pair.
    /// </summary>
    /// <param name="score">A geohash integer previously produced by <see cref="EncodeGeoHash"/>.</param>
    /// <returns>A tuple of <c>(longitude, latitude)</c> in degrees.</returns>
    public static (double lon, double lat) DecodeGeoHash(long score)
    {
        long latBits = 0, lonBits = 0;
        for (int i = 0; i < 26; i++)
        {
            latBits |= ((score >> (2 * i)) & 1L) << i;
            lonBits |= ((score >> (2 * i + 1)) & 1L) << i;
        }

        double lon = (lonBits + 0.5) / (double)(1L << 26) * 360.0 - 180.0;
        double lat = (latBits + 0.5) / (double)(1L << 26) * 170.10225756 - 85.05112878;
        return (lon, lat);
    }

    /// <summary>
    /// Calculates the great-circle distance in meters between two geographic coordinates
    /// using the Haversine formula and Redis's earth-radius constant of 6372797.560856 m.
    /// </summary>
    /// <param name="lat1Deg">Latitude of the first point in degrees.</param>
    /// <param name="lon1Deg">Longitude of the first point in degrees.</param>
    /// <param name="lat2Deg">Latitude of the second point in degrees.</param>
    /// <param name="lon2Deg">Longitude of the second point in degrees.</param>
    /// <returns>Distance in meters.</returns>
    public static double DistanceMeters(double lat1Deg, double lon1Deg, double lat2Deg, double lon2Deg)
    {
        double lat1 = lat1Deg * Math.PI / 180.0;
        double lat2 = lat2Deg * Math.PI / 180.0;
        double dLat = (lat2Deg - lat1Deg) * Math.PI / 180.0;
        double dLon = (lon2Deg - lon1Deg) * Math.PI / 180.0;

        double a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2)
                 + Math.Cos(lat1) * Math.Cos(lat2)
                 * Math.Sin(dLon / 2) * Math.Sin(dLon / 2);

        return EarthRadiusMeters * 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
    }
}
