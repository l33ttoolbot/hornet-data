"""
Utility functions for the Hornet-Data Pipeline.

This module provides:
- Rate limiting (token bucket)
- Retry with exponential backoff
- SHA256 hash computation
- License validation
- HTTP helpers
"""

import hashlib
import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Callable, Optional, Tuple, Any
from functools import wraps

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


# === Rate Limiting ===

class RateLimiter:
    """
    Token bucket rate limiter for API compliance.
    
    Thread-safe implementation that ensures requests don't exceed
    a specified rate per second.
    """
    
    def __init__(self, requests_per_second: float):
        """
        Initialize rate limiter.
        
        Args:
            requests_per_second: Maximum requests allowed per second
        """
        self.min_interval = 1.0 / requests_per_second
        self.last_request = 0.0
        self.lock = Lock()
    
    def wait(self):
        """Block until rate limit allows next request."""
        with self.lock:
            now = time.time()
            elapsed = now - self.last_request
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                time.sleep(sleep_time)
            self.last_request = time.time()
    
    def acquire(self) -> float:
        """
        Acquire permission to make a request.
        
        Returns:
            Time waited in seconds
        """
        with self.lock:
            now = time.time()
            elapsed = now - self.last_request
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                time.sleep(sleep_time)
                self.last_request = time.time()
                return sleep_time
            self.last_request = time.time()
            return 0.0


# === Retry Logic ===

@dataclass
class RetryConfig:
    """Configuration for retry with exponential backoff."""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True


def retry_with_backoff(
    func: Callable[[], Any],
    config: Optional[RetryConfig] = None,
    retryable_exceptions: Tuple = (TimeoutError, ConnectionError, requests.RequestException),
    on_retry: Optional[Callable[[int, Exception], None]] = None
) -> Any:
    """
    Execute function with exponential backoff retry.
    
    Args:
        func: Function to execute
        config: Retry configuration (uses defaults if None)
        retryable_exceptions: Exceptions that trigger retry
        on_retry: Callback for retry attempts (attempt, exception)
    
    Returns:
        Result of the function
    
    Raises:
        Last exception if all retries exhausted
    """
    if config is None:
        config = RetryConfig()
    
    last_exception = None
    
    for attempt in range(config.max_retries + 1):
        try:
            return func()
        except retryable_exceptions as e:
            last_exception = e
            
            if attempt < config.max_retries:
                # Calculate delay with exponential backoff
                delay = min(
                    config.base_delay * (config.exponential_base ** attempt),
                    config.max_delay
                )
                
                # Add jitter to prevent thundering herd
                if config.jitter:
                    delay *= (0.5 + random.random())
                
                logger.warning(
                    f"Attempt {attempt + 1}/{config.max_retries + 1} failed: {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                
                if on_retry:
                    on_retry(attempt, e)
                
                time.sleep(delay)
            else:
                logger.error(f"All {config.max_retries + 1} retries exhausted for {func}")
    
    raise last_exception


def with_retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    retryable_exceptions: Tuple = (TimeoutError, ConnectionError, requests.RequestException)
):
    """
    Decorator for retry with exponential backoff.
    
    Usage:
        @with_retry(max_retries=3)
        def my_api_call():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            config = RetryConfig(max_retries=max_retries, base_delay=base_delay)
            return retry_with_backoff(
                lambda: func(*args, **kwargs),
                config=config,
                retryable_exceptions=retryable_exceptions
            )
        return wrapper
    return decorator


# === Hash Utilities ===

def compute_sha256(file_path: str | Path) -> str:
    """
    Compute SHA256 hash of a file.
    
    Args:
        file_path: Path to the file
    
    Returns:
        Hexadecimal SHA256 hash string
    """
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_sha256_bytes(data: bytes) -> str:
    """
    Compute SHA256 hash of bytes.
    
    Args:
        data: Bytes to hash
    
    Returns:
        Hexadecimal SHA256 hash string
    """
    return hashlib.sha256(data).hexdigest()


# === License Validation ===

# License type mappings
LICENSE_WHITELIST = {
    "CC0": ["CC0", "cc0", "CC0 1.0", "CC0-1.0", "Public Domain", "public domain"],
    "CC-BY": ["CC-BY", "cc-by", "CC BY", "CC BY 4.0", "CC-BY-4.0", "CC-BY-3.0", "Attribution"],
    "CC-BY-SA": ["CC-BY-SA", "cc-by-sa", "CC BY-SA", "CC BY-SA 4.0", "CC-BY-SA-4.0"],
    "CC-BY-NC": ["CC-BY-NC", "cc-by-nc", "CC BY-NC", "CC BY-NC 4.0", "CC-BY-NC-4.0"],
    "unknown": ["unknown", "Unknown", None, ""],
}

# Reverse mapping for normalization
LICENSE_NORMALIZE = {}
for normalized, variants in LICENSE_WHITELIST.items():
    for variant in variants:
        if variant:
            LICENSE_NORMALIZE[variant.lower()] = normalized


def normalize_license(license_str: Optional[str]) -> str:
    """
    Normalize a license string to a standard form.
    
    Args:
        license_str: Raw license string (may be None or empty)
    
    Returns:
        Normalized license string (CC0, CC-BY, CC-BY-SA, CC-BY-NC, or unknown)
    """
    if not license_str:
        return "unknown"
    
    normalized = LICENSE_NORMALIZE.get(license_str.lower())
    if normalized:
        return normalized
    
    # Try partial matching
    license_lower = license_str.lower()
    if "cc0" in license_lower or "public domain" in license_lower:
        return "CC0"
    if "cc-by-sa" in license_lower:
        return "CC-BY-SA"
    if "cc-by-nc" in license_lower:
        return "CC-BY-NC"
    if "cc-by" in license_lower or "attribution" in license_lower:
        return "CC-BY"
    
    return "unknown"


def is_license_allowed(license_str: Optional[str], whitelist: list) -> bool:
    """
    Check if a license is in the allowed whitelist.
    
    Args:
        license_str: License string to check
        whitelist: List of allowed license types
    
    Returns:
        True if license is allowed
    """
    normalized = normalize_license(license_str)
    return normalized in whitelist


class LicenseValidator:
    """Validates licenses against a whitelist."""
    
    def __init__(self, whitelist: list):
        """
        Initialize validator with allowed licenses.
        
        Args:
            whitelist: List of allowed license types (e.g., ["CC0", "CC-BY"])
        """
        self.whitelist = whitelist
    
    def is_allowed(self, license_str: Optional[str]) -> bool:
        """Check if a license is allowed."""
        return is_license_allowed(license_str, self.whitelist)
    
    def normalize(self, license_str: Optional[str]) -> str:
        """Normalize a license string."""
        return normalize_license(license_str)


# === HTTP Utilities ===

def create_session(
    timeout: int = 30,
    retries: int = 3,
    backoff_factor: float = 0.5,
    pool_connections: int = 10,
    pool_maxsize: int = 10
) -> requests.Session:
    """
    Create a configured requests session with retry logic.
    
    Args:
        timeout: Default timeout for requests
        retries: Number of retries
        backoff_factor: Backoff factor for retries
        pool_connections: Connection pool size
        pool_maxsize: Maximum pool size
    
    Returns:
        Configured requests Session
    """
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session


def check_url_exists(url: str, session: Optional[requests.Session] = None, timeout: int = 10) -> Tuple[bool, Optional[dict]]:
    """
    Check if a URL exists via HEAD request.
    
    Args:
        url: URL to check
        session: Optional requests session
        timeout: Request timeout
    
    Returns:
        Tuple of (exists: bool, headers: dict|None)
    """
    sess = session or create_session()
    try:
        response = sess.head(url, timeout=timeout, allow_redirects=True)
        if response.status_code == 200:
            headers = {
                "content_type": response.headers.get("Content-Type", ""),
                "content_length": int(response.headers.get("Content-Length", 0)),
            }
            return True, headers
        return False, None
    except requests.RequestException:
        return False, None


def download_file(
    url: str,
    output_path: str | Path,
    session: Optional[requests.Session] = None,
    timeout: int = 30,
    chunk_size: int = 8192
) -> Tuple[bool, int]:
    """
    Download a file to disk.
    
    Args:
        url: URL to download
        output_path: Path to save the file
        session: Optional requests session
        timeout: Request timeout
        chunk_size: Download chunk size
    
    Returns:
        Tuple of (success: bool, bytes_downloaded: int)
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    sess = session or create_session()
    
    try:
        response = sess.get(url, timeout=timeout, stream=True)
        response.raise_for_status()
        
        total_bytes = 0
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    total_bytes += len(chunk)
        
        return True, total_bytes
    
    except requests.RequestException as e:
        logger.error(f"Failed to download {url}: {e}")
        # Clean up partial file
        if output_path.exists():
            output_path.unlink()
        return False, 0


# === URL Generation ===

def generate_lubw_url(year: int, month: int, number: int, ext: str = "jpg") -> str:
    """
    Generate a LUBW/Convotis URL.
    
    URL Pattern: https://gmp.convotis.com/documents/d/global/anhang_YYYY-MM-NNNN-ext
    
    Args:
        year: Year (e.g., 2023)
        month: Month (1-12)
        number: Document number (1-9999)
        ext: File extension (default: jpg)
    
    Returns:
        Full URL
    """
    return f"https://gmp.convotis.com/documents/d/global/anhang_{year:04d}-{month:02d}-{number:04d}.{ext}"


# === File Utilities ===

def get_file_extension(content_type: str) -> str:
    """
    Get file extension from Content-Type header.
    
    Args:
        content_type: Content-Type header value
    
    Returns:
        File extension (without dot)
    """
    content_type = content_type.lower()
    
    if "jpeg" in content_type or "jpg" in content_type:
        return "jpg"
    if "png" in content_type:
        return "png"
    if "gif" in content_type:
        return "gif"
    if "webp" in content_type:
        return "webp"
    if "bmp" in content_type:
        return "bmp"
    if "tiff" in content_type:
        return "tiff"
    
    return "jpg"  # Default


def humanize_bytes(size_bytes: int) -> str:
    """
    Convert bytes to human-readable string.
    
    Args:
        size_bytes: Size in bytes
    
    Returns:
        Human-readable string (e.g., "1.5 MB")
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"