"""Outbound URL policy: refuse a URL that would make the runner connect to a
loopback, private, link-local (cloud metadata) or otherwise reserved address.

Shared by the diagnostic-URL forwarding in webhooks.py and, when
EPHEMERAL_BASE_URL_BLOCK_PRIVATE_ADDRESSES is on, the draft base_url check on
the ephemeral path in action_runner.py.

This is a best-effort defence. httpx re-resolves DNS at request time, so a
rebinding attack can still reach a private address after the check passes
(TOCTOU); restrict outbound network access at the infrastructure level as
well for a complete mitigation.
"""
import asyncio
import ipaddress
from typing import Iterable
from urllib.parse import urlparse

# Bound on the resolver call. httpx's timeout is per phase of its own request
# and does not cover getaddrinfo, which runs in the default executor with no
# deadline of its own.
DNS_RESOLUTION_TIMEOUT_SECONDS = 5.0

BLOCKED_NETWORKS = [
    ipaddress.ip_network("127.0.0.0/8"),    # loopback
    ipaddress.ip_network("::1/128"),          # IPv6 loopback
    ipaddress.ip_network("10.0.0.0/8"),       # RFC 1918 private
    ipaddress.ip_network("172.16.0.0/12"),    # RFC 1918 private
    ipaddress.ip_network("192.168.0.0/16"),   # RFC 1918 private
    ipaddress.ip_network("169.254.0.0/16"),   # link-local / cloud metadata (AWS, GCP)
    ipaddress.ip_network("fe80::/10"),        # IPv6 link-local
    ipaddress.ip_network("fec0::/10"),        # deprecated IPv6 site-local; ipaddress still reports it is_global
    ipaddress.ip_network("fc00::/7"),         # IPv6 unique local
    ipaddress.ip_network("0.0.0.0/8"),        # unspecified
    ipaddress.ip_network("100.64.0.0/10"),    # carrier-grade NAT
    ipaddress.ip_network("224.0.0.0/4"),      # multicast
    ipaddress.ip_network("240.0.0.0/4"),      # reserved
    ipaddress.ip_network("::/128"),           # IPv6 unspecified
    ipaddress.ip_network("ff00::/8"),         # IPv6 multicast
    # IANA special-purpose ranges that ipaddress.is_global still reports as
    # global on the Python the image runs (3.10; corrected in 3.13). Listed so
    # the policy does not depend on the interpreter's registry. 192.0.0.0/24 is
    # blocked whole: its two globally reachable anycast hosts (.9, .10) are
    # not addresses a source system lives at.
    ipaddress.ip_network("192.0.0.0/24"),     # IETF protocol assignments (incl. the dummy 192.0.0.8)
    ipaddress.ip_network("192.31.196.0/24"),  # AS112-v4
    ipaddress.ip_network("192.52.193.0/24"),  # AMT
    ipaddress.ip_network("192.88.99.0/24"),   # deprecated 6to4 relay anycast
    ipaddress.ip_network("192.175.48.0/24"),  # direct delegation AS112
    ipaddress.ip_network("64:ff9b:1::/48"),   # local-use IPv4/IPv6 translation
    ipaddress.ip_network("2002::/16"),        # 6to4
    # Registry entries newer than any pinned-era ipaddress knows about. The
    # IANA special-purpose registries are the source of truth for this list;
    # re-check them when bumping the Python image.
    ipaddress.ip_network("3fff::/20"),        # documentation (RFC 9637)
    ipaddress.ip_network("5f00::/16"),        # SRv6 SIDs (RFC 9602)
]


async def _resolve_addresses(hostname: str) -> list:
    """Every address `hostname` resolves to, as strings, within
    DNS_RESOLUTION_TIMEOUT_SECONDS. Tests replace this function rather than
    asyncio.get_running_loop: patching that module attribute also reaches the
    test client's own event loop machinery."""
    loop = asyncio.get_running_loop()
    addr_infos = await asyncio.wait_for(
        loop.getaddrinfo(hostname, None), timeout=DNS_RESOLUTION_TIMEOUT_SECONDS,
    )
    return [sockaddr[0] for _, _, _, _, sockaddr in addr_infos]


async def validate_outbound_url(url: str, *, allowlist: Iterable[str] = (), what: str = "URL") -> None:
    """Raise ValueError unless `url` is https, has a hostname, is in `allowlist`
    when one is given, and resolves only to public addresses.

    `what` names the URL in the messages ("diagnostic URL", "draft base_url");
    they carry the hostname and the resolved address, never the URL's path,
    query or userinfo, so they are safe to surface.
    """
    lead = what[:1].upper() + what[1:]
    try:
        parsed = urlparse(url)
        hostname = (parsed.hostname or "").rstrip(".").lower()
    except ValueError:
        # The parser's own message quotes the authority it choked on, userinfo
        # included (e.g. a fullwidth "@" fails NFKC validation with the whole
        # netloc in the text). Callers treat this function's ValueErrors as
        # safe, policy-authored text, so never let that message through.
        raise ValueError(f"{lead} could not be parsed.") from None
    if parsed.scheme != "https":
        raise ValueError(f"{lead} scheme '{parsed.scheme}' is not allowed; only 'https' is permitted.")
    if not hostname:
        raise ValueError(f"{lead} has no hostname.")
    allowed = [h.rstrip(".").lower() for h in allowlist]
    if allowed and hostname not in allowed:
        raise ValueError(f"{lead} hostname '{hostname}' is not in the configured allowlist.")
    try:
        addresses = await _resolve_addresses(hostname)
    except asyncio.TimeoutError:
        raise ValueError(
            f"Timed out resolving {what} hostname '{hostname}' after {DNS_RESOLUTION_TIMEOUT_SECONDS:g}s."
        )
    except OSError as e:
        raise ValueError(f"Cannot resolve {what} hostname '{hostname}': {e}")
    for address in addresses:
        ip = ipaddress.ip_address(address)
        # An IPv4-mapped IPv6 address (::ffff:a.b.c.d) parses as IPv6 and would
        # sail past the IPv4 blocklist entries; check the embedded IPv4 instead.
        if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
            ip = ip.ipv4_mapped
        # The explicit blocklist names the ranges an operator expects to see;
        # is_global catches the rest of the special-use space (benchmarking,
        # documentation and other IANA-reserved ranges are often routed
        # internally) so the check means "public addresses only" literally.
        if not ip.is_global or any(ip in net for net in BLOCKED_NETWORKS):
            raise ValueError(
                f"{lead} resolves to a private or reserved address ({ip}), which is blocked to prevent SSRF."
            )
