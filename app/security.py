from __future__ import annotations
import os
from typing import Optional, Set, Dict, Any
from fastapi import Header, HTTPException, Request
from ipaddress import ip_address, ip_network
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(), override=False)

# ---- Config from env (.env.template you shared) 
TOKENS_DEFAULT: Set[str] = {
    t.strip() for t in os.getenv("API_TOKENS_DEFAULT", "").split(",") if t.strip()
}
TOKENS_PII: Set[str] = {
    t.strip() for t in os.getenv("API_TOKENS_PII", "").split(",") if t.strip()
}

_ip_raw = os.getenv("API_IP_ALLOWLIST", "") or ""
IP_ALLOWLIST = [cidr.strip() for cidr in _ip_raw.split(",") if cidr.strip()]

def _client_ip(request: Request) -> Optional[str]:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else None

def _ip_allowed(ip: Optional[str]) -> bool:
    if not IP_ALLOWLIST:
        return True  # allow all if no CIDRs configured
    if not ip:
        return False
    try:
        ip_obj = ip_address(ip)
        return any(ip_obj in ip_network(cidr, strict=False) for cidr in IP_ALLOWLIST)
    except Exception:
        return False

class AuthContext(Dict[str, Any]):
    @property
    def roles(self) -> Set[str]:
        return self.get("roles", set())
    def has_role(self, role: str) -> bool:
        return role in self.roles

async def require_token(request: Request, authorization: Optional[str] = Header(None)) -> AuthContext:
    ip = _client_ip(request)
    if not _ip_allowed(ip):
        raise HTTPException(status_code=403, detail="forbidden: ip_not_allowed")

    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="unauthorized: missing_bearer_token")
    token = authorization.split(" ", 1)[1].strip()

    roles: Set[str] = set()
    actor = "unknown"
    if token in TOKENS_PII:
        roles.update({"default", "pii_reader"})
        actor = "pii_token"
    elif token in TOKENS_DEFAULT:
        roles.add("default")
        actor = "default_token"
    else:
        raise HTTPException(status_code=401, detail="unauthorized: invalid_token")

    # expose to middleware via request.state
    request.state.actor = actor
    request.state.ip = ip
    return AuthContext(actor=actor, roles=roles, ip=ip)

def allow_pii(ctx: AuthContext) -> bool:
    return ctx.has_role("pii_reader")
