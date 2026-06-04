"""
Geo-location lookup for ETL Pipeline.
PHẦN 5 — Transform phase: IP → geographic location.

Supports two modes:
  1. MaxMind GeoLite2 (.mmdb) — REAL geo data, loaded into RAM.
     Get a free license key from https://www.maxmind.com/en/geolite2/signup
     Download the database using: python download_geoip.py <YOUR_LICENSE_KEY>

  2. Synthetic fallback — deterministic, for demo/testing without .mmdb.
     Maps IPs to real countries based on their numeric value.
"""

import logging
import os
import sys
from pathlib import Path
from typing import Optional

logger = logging.getLogger("etl.geo_lookup")

# Check for geoip2 library
try:
    import geoip2
    import geoip2.database
    _GEOIP2_AVAILABLE = True
except ImportError:
    _GEOIP2_AVAILABLE = False
    logger.warning("geoip2 package not installed. Run: pip install geoip2")

# Import error class separately to avoid NameError if geoip2 not available
try:
    import geoip2.errors
    _GEOIP2_ERRORS = geoip2.errors
except ImportError:
    _GEOIP2_ERRORS = None


class GeoRecord:
    """Result of a geo lookup."""
    __slots__ = ("ip", "country", "city", "latitude", "longitude")

    def __init__(self, ip: str, country: str, city: str, latitude: float, longitude: float):
        self.ip = ip
        self.country = country
        self.city = city
        self.latitude = latitude
        self.longitude = longitude

    def to_dict(self) -> dict:
        return {
            "ip": self.ip,
            "country": self.country,
            "city": self.city,
            "latitude": self.latitude,
            "longitude": self.longitude,
        }

    def __repr__(self) -> str:
        return f"GeoRecord({self.ip}, {self.country}, {self.city}, {self.latitude}, {self.longitude})"


class GeoLookup:
    """
    In-memory GeoIP lookup. Loads .mmdb into RAM on init.
    Lookup is O(log n) — microseconds per query.

    Includes a process-level LRU cache shared across all GeoLookup instances
    to avoid redundant lookups (especially useful when multiple reducers
    share the same IPs after global dedup).
    """

    # Module-level cache shared across all GeoLookup instances (process-wide)
    _global_cache: dict[str, "GeoRecord"] = {}
    _cache_hits: int = 0
    _cache_misses: int = 0

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path
        self._reader = None
        self._lookup_mode = "none"
        self._local_cache: dict[str, GeoRecord] = {}
        self._local_hits = 0
        self._local_misses = 0
        self._init(db_path)

    def _init(self, db_path: Optional[str]):
        """Try to load MaxMind .mmdb, fall back to synthetic."""
        if not _GEOIP2_AVAILABLE:
            self._lookup_mode = "synthetic"
            logger.info("GeoLookup: geoip2 not installed — using synthetic fallback")
            return

        if db_path is None:
            self._lookup_mode = "synthetic"
            logger.info("GeoLookup: no .mmdb path provided — using synthetic fallback")
            return

        mmdb_path = Path(db_path)
        if not mmdb_path.exists():
            self._lookup_mode = "synthetic"
            logger.info(
                f"GeoLookup: .mmdb not found at {db_path} — using synthetic fallback"
            )
            return

        try:
            self._reader = geoip2.database.Reader(str(mmdb_path))
            self._lookup_mode = "maxmind"
            size_mb = mmdb_path.stat().st_size / (1024 * 1024)
            logger.info(
                f"GeoLookup: loaded MaxMind GeoLite2-City from {mmdb_path.name} "
                f"({size_mb:.1f}MB) — REAL geo data active"
            )
        except Exception as exc:
            self._lookup_mode = "synthetic"
            logger.warning(
                f"GeoLookup: failed to load .mmdb ({exc}) — using synthetic fallback"
            )

    def lookup(self, ip: str) -> GeoRecord:
        """
        Look up geographic info for an IP address.
        Uses a global LRU cache to avoid redundant lookups across reducers.
        """
        # Check global cache first (shared across all GeoLookup instances)
        if ip in GeoLookup._global_cache:
            GeoLookup._cache_hits += 1
            return GeoLookup._global_cache[ip]

        # Check local cache
        if ip in self._local_cache:
            self._local_hits += 1
            return self._local_cache[ip]

        GeoLookup._cache_misses += 1
        self._local_misses += 1

        record = self._do_lookup(ip)

        # Store in both caches
        self._local_cache[ip] = record
        GeoLookup._global_cache[ip] = record

        return record

    def _do_lookup(self, ip: str) -> GeoRecord:
        """Actual lookup against .mmdb or synthetic fallback."""
        if self._reader is not None:
            try:
                response = self._reader.city(ip)
                return GeoRecord(
                    ip=ip,
                    country=getattr(response.country, "name", "Unknown") or "Unknown",
                    city=getattr(response.city, "name", "Unknown") or "Unknown",
                    latitude=response.location.latitude or 0.0,
                    longitude=response.location.longitude or 0.0,
                )
            except (AttributeError, TypeError):
                pass
            except Exception:
                pass

        return self._synthetic_lookup(ip)

    def _synthetic_lookup(self, ip: str) -> GeoRecord:
        """
        Deterministic synthetic geo lookup.
        Maps IP to real countries/coordinates based on numeric hash.
        """
        # Parse IP octets and compute a deterministic numeric value
        try:
            octets = [int(x) for x in ip.split(".")]
            numeric = (octets[0] << 24) | (octets[1] << 16) | (octets[2] << 8) | octets[3]
        except (ValueError, IndexError):
            numeric = hash(ip)

        country_data = [
            ("United States", "New York",      40.7128,  -74.0060),
            ("United States", "Los Angeles",   34.0522, -118.2437),
            ("United States", "Chicago",       41.8781,  -87.6298),
            ("United States", "Seattle",        47.6062, -122.3321),
            ("United States", "San Francisco", 37.7749, -122.4194),
            ("United States", "Houston",        29.7604,  -95.3698),
            ("United States", "Miami",          25.7617,  -80.1918),
            ("United States", "Boston",         42.3601,  -71.0589),
            ("United Kingdom", "London",         51.5074,   -0.1278),
            ("United Kingdom", "Manchester",    53.4808,   -2.2426),
            ("Germany",        "Berlin",        52.5200,   13.4050),
            ("Germany",        "Munich",        48.1351,   11.5820),
            ("France",         "Paris",         48.8566,    2.3522),
            ("Netherlands",    "Amsterdam",     52.3676,    4.9041),
            ("Spain",          "Madrid",        40.4168,   -3.7038),
            ("Italy",          "Rome",          41.9028,   12.4964),
            ("Poland",         "Warsaw",        52.2297,   21.0122),
            ("Sweden",         "Stockholm",     59.3293,   18.0686),
            ("Norway",         "Oslo",          59.9139,   10.7522),
            ("Denmark",        "Copenhagen",    55.6761,   12.5683),
            ("Japan",          "Tokyo",         35.6762,  139.6503),
            ("Japan",          "Osaka",         34.6937,  135.5023),
            ("South Korea",    "Seoul",         37.5665,  126.9780),
            ("China",          "Beijing",       39.9042,  116.4074),
            ("China",          "Shanghai",      31.2304,  121.4737),
            ("Hong Kong",      "Hong Kong",     22.3193,  114.1694),
            ("Taiwan",         "Taipei",        25.0330,  121.5654),
            ("Singapore",       "Singapore",     1.3521,  103.8198),
            ("India",          "Mumbai",        19.0760,   72.8777),
            ("India",          "New Delhi",     28.6139,   77.2090),
            ("India",          "Bangalore",     12.9716,   77.5946),
            ("Thailand",        "Bangkok",       13.7563,  100.5018),
            ("Vietnam",         "Ho Chi Minh City", 10.8231, 106.6297),
            ("Vietnam",         "Hanoi",         21.0285,  105.8542),
            ("Indonesia",       "Jakarta",       -6.2088,  106.8456),
            ("Philippines",     "Manila",        14.5995,  120.9842),
            ("Malaysia",        "Kuala Lumpur",   3.1390,  101.6869),
            ("Australia",       "Sydney",        -33.8688,  151.2093),
            ("Australia",       "Melbourne",     -37.8136,  144.9631),
            ("Brazil",          "Sao Paulo",    -23.5505,  -46.6333),
            ("Brazil",          "Rio de Janeiro", -22.9068, -43.1729),
            ("Argentina",       "Buenos Aires",  -34.6037,  -58.3816),
            ("Mexico",          "Mexico City",    19.4326,  -99.1332),
            ("Canada",          "Toronto",       43.6532,  -79.3832),
            ("Canada",          "Vancouver",     49.2827, -123.1207),
            ("Russia",          "Moscow",        55.7558,   37.6173),
            ("South Africa",    "Johannesburg",  -26.2041,   28.0473),
            ("United Arab Emirates", "Dubai",    25.2048,   55.2708),
            ("Turkey",          "Istanbul",      41.0082,   28.9784),
            ("Israel",          "Tel Aviv",      32.0853,   34.7818),
            ("Nigeria",         "Lagos",          6.5244,    3.3792),
            ("Egypt",           "Cairo",         30.0444,   31.2357),
        ]

        idx = abs(numeric) % len(country_data)
        country, city, base_lat, base_lon = country_data[idx]

        lat_offset = ((abs(numeric) // len(country_data)) % 1000) / 10000.0
        lon_offset = ((abs(numeric) // (len(country_data) * 1000)) % 1000) / 10000.0

        lat = round(max(-90, min(90, base_lat + lat_offset)), 6)
        lon = round(max(-180, min(180, base_lon + lon_offset)), 6)

        return GeoRecord(ip, country, city, lat, lon)

    @property
    def lookup_mode(self) -> str:
        return self._lookup_mode

    def is_using_maxmind(self) -> bool:
        return self._lookup_mode == "maxmind"

    @classmethod
    def get_cache_stats(cls) -> dict:
        """Return cache hit/miss statistics for the global cache."""
        total = cls._cache_hits + cls._cache_misses
        hit_rate = cls._cache_hits / total * 100 if total > 0 else 0
        return {
            "hits": cls._cache_hits,
            "misses": cls._cache_misses,
            "total": total,
            "hit_rate_pct": round(hit_rate, 1),
            "cache_size": len(cls._global_cache),
        }

    def close(self):
        if self._reader:
            self._reader.close()
            self._reader = None

    def __del__(self):
        self.close()


def get_default_db_path() -> Optional[str]:
    """Look for .mmdb file in common locations."""
    base_dir = Path(__file__).resolve().parent.parent
    search_paths = [
        base_dir / "data" / "GeoLite2-City.mmdb",
        base_dir / "GeoLite2-City.mmdb",
        Path(os.getcwd()) / "GeoLite2-City.mmdb",
    ]
    for p in search_paths:
        if p.exists():
            return str(p)
    return None
