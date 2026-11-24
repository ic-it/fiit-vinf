"""Explicit transports and durable, thread-safe site/proxy admission policies."""

import hashlib
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from protego import Protego
from pydantic import Field
from vinf_core.files import atomic_model, read_model
from vinf_core.models import Model, PauseState, VinfError

from vinf_crawler.config import CrawlConfig, RateLimitScope, RobotsConfig
from vinf_crawler.discovery import origin
from vinf_crawler.proxies import read_proxies


@dataclass(frozen=True)
class Transport:
    id: str
    config: CrawlConfig
    proxy_url: str | None = field(default=None, repr=False)
    pooled: bool = False


def transports(config: CrawlConfig, root: Path | None = None) -> tuple[Transport, ...]:
    if not config.proxies and config.proxy_pool is None:
        return (Transport("direct", config),)
    result = []
    for proxy in sorted(config.proxies, key=lambda value: value.id):
        value = os.environ.get(proxy.url_env, "").strip()
        try:
            parsed = urlsplit(value)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.hostname
                or parsed.path not in {"", "/"}
                or parsed.query
                or parsed.fragment
                or parsed.port == 0
                or any(char in value for char in "\r\n")
            ):
                raise ValueError
        except ValueError:
            raise VinfError(
                f"Set {proxy.url_env} to a valid HTTP(S) proxy URL for {proxy.id}."
            ) from None
        http = (
            config.http
            if proxy.delay_seconds is None
            else config.http.model_copy(update={"delay_seconds": proxy.delay_seconds})
        )
        settings = config.model_copy(
            update={
                "http": http,
                "robots": RobotsConfig(
                    strategy=proxy.robots_strategy or config.robots.strategy
                ),
                "failures": proxy.failures or config.failures,
            }
        )
        result.append(Transport(proxy.id, settings, value))
    if pool := config.proxy_pool:
        path = Path(pool.file)
        if not path.is_absolute():
            path = (root or Path.cwd()) / path
        for url in read_proxies(path).urls[: pool.size]:
            identifier = "pool-" + hashlib.sha256(url.encode()).hexdigest()[:12]
            settings = config.model_copy(
                update={
                    "http": config.http.model_copy(
                        update={
                            "connect_timeout": pool.connect_timeout,
                            "read_timeout": pool.read_timeout,
                        }
                    )
                }
            )
            result.append(Transport(identifier, settings, url, pooled=True))
        if pool.fallback_direct:
            result.append(Transport("direct", config))
    if not result:
        raise VinfError(
            "No proxy transports available; check the pool or enable direct fallback."
        )
    return tuple(result)


class OriginThrottle(Model):
    last_start: float = 0
    starts: tuple[float, ...] = ()
    cooldown_until: float = 0


class TransportState(Model):
    origins: dict[str, OriginThrottle] = Field(default_factory=dict)
    pause: PauseState | None = None
    disabled: str | None = None


class NetworkState(Model):
    origins: dict[str, OriginThrottle] = Field(default_factory=dict)
    transports: dict[str, TransportState] = Field(default_factory=dict)


class RobotsSnapshot(Model):
    origin: str
    url: str
    status: int
    body: str
    fetched_at: float
    expires_at: float


class RequestDeferred(VinfError):
    """Admission stopped; retain its deadline without counting a failed attempt."""

    def __init__(self, message: str, retry_at: float | None):
        super().__init__(message)
        self.retry_at = retry_at


class FetchCancelled(VinfError):
    pass


def window_wait(window: object | None) -> float:
    start, end = getattr(window, "start_time", None), getattr(window, "end_time", None)
    if start is None or end is None or start == end:
        return 0
    now = datetime.now(UTC)
    clock = now.timetz().replace(tzinfo=None)
    if (start <= clock < end) if start < end else (clock >= start or clock < end):
        return 0
    begins = datetime.combine(now.date(), start, UTC).timestamp()
    return begins + (86400 if begins <= now.timestamp() else 0) - now.timestamp()


def _eligible(
    history: OriginThrottle,
    delay: float,
    rules: tuple[Protego, ...],
    user_agent: str,
    now: float,
) -> float:
    eligible = history.last_start + delay
    for rule in rules:
        eligible = max(
            eligible, history.last_start + (rule.crawl_delay(user_agent) or 0)
        )
        rate = rule.request_rate(user_agent)
        eligible = max(
            eligible,
            now + window_wait(rule.visit_time(user_agent)),
            now + window_wait(rate),
        )
        if rate is not None and rate.requests > 0 and rate.seconds > 0:
            recent = [stamp for stamp in history.starts if stamp > now - rate.seconds]
            if len(recent) >= rate.requests:
                eligible = max(eligible, recent[-rate.requests] + rate.seconds + 0.001)
    return eligible


def _started(
    history: OriginThrottle, now: float, rules: tuple[Protego, ...], user_agent: str
) -> OriginThrottle:
    retention = max(
        (
            rule.request_rate(user_agent).seconds
            for rule in rules
            if rule.request_rate(user_agent) is not None
        ),
        default=0,
    )
    retention = max(86400, retention)
    return history.model_copy(
        update={
            "last_start": now,
            "starts": tuple(
                stamp for stamp in history.starts if stamp > now - retention
            )
            + (now,),
        }
    )


class NetworkPolicy:
    """Atomic site/transport clocks, selectable pacing and shared retry cooldowns."""

    def __init__(
        self,
        root: Path,
        config: CrawlConfig,
        routes: tuple[Transport, ...],
        report,
    ):
        self.root, self.config, self.report = root, config, report
        self.path = root / "crawl/network.json"
        self.state = (
            read_model(self.path, NetworkState)
            if self.path.exists()
            else NetworkState()
        )
        self.routes = {route.id: route for route in routes}
        self.guard = threading.RLock()
        self.cancelled = threading.Event()
        self.robot_guards: dict[tuple[str, str], threading.Lock] = {}
        self.rules: dict[tuple[str, str], Protego] = {}
        for route in routes:
            self.state.transports.setdefault(route.id, TransportState())
            if route.config.robots.strategy.respects_timing:
                for path in self.robots_directory(route.id).glob("*.json"):
                    saved = read_model(path, RobotsSnapshot)
                    self.rules[route.id, saved.origin] = Protego.parse(saved.body)

    def robots_directory(self, proxy_id: str) -> Path:
        return self.root / "crawl/transports" / proxy_id / "robots"

    def robots_lock(self, proxy_id: str, host: str):
        with self.guard:
            return self.robot_guards.setdefault((proxy_id, host), threading.Lock())

    def register_rules(self, proxy_id: str, host: str, rules: Protego) -> None:
        with self.guard:
            if self.routes[proxy_id].config.robots.strategy.respects_timing:
                self.rules[proxy_id, host] = rules

    def _save(self) -> None:
        atomic_model(self.path, self.state)

    def ready_at(self, proxy_id: str, url: str) -> float | None:
        with self.guard:
            state = self.state.transports[proxy_id]
            if state.disabled:
                return None
            if state.pause:
                if state.pause.until is None:
                    return None
                if state.pause.until > time.time():
                    return state.pause.until
                self.state.transports[proxy_id] = state = state.model_copy(
                    update={"pause": None}
                )
                self._save()
            host = origin(url)
            return max(
                state.origins.get(host, OriginThrottle()).cooldown_until,
                self.state.origins.get(host, OriginThrottle()).cooldown_until,
            )

    def before_request(self, proxy_id: str, url: str, rules: Protego | None) -> None:
        host, reported = origin(url), False
        route = self.routes[proxy_id]
        local_rules = (
            (rules,)
            if rules is not None and route.config.robots.strategy.respects_timing
            else ()
        )
        while True:
            if self.cancelled.is_set():
                raise FetchCancelled("Crawl is stopping.")
            with self.guard:
                now = time.time()
                ready = self.ready_at(proxy_id, url)
                if ready is None or ready > now:
                    raise RequestDeferred(
                        f"{proxy_id}: cooldown/pause active for {host}; task retained.",
                        ready,
                    )
                global_history = self.state.origins.get(host, OriginThrottle())
                local_state = self.state.transports[proxy_id]
                local_history = local_state.origins.get(host, OriginThrottle())
                global_rules = tuple(
                    rule for (_, site), rule in self.rules.items() if site == host
                )
                configured_eligible = (
                    local_history.last_start + route.config.http.delay_seconds
                )
                eligible = _eligible(
                    local_history,
                    route.config.http.delay_seconds,
                    local_rules,
                    route.config.http.user_agent,
                    now,
                )
                if self.config.http.rate_limit_scope == RateLimitScope.SITE:
                    configured_eligible = max(
                        configured_eligible,
                        global_history.last_start + self.config.http.delay_seconds,
                    )
                    eligible = max(
                        eligible,
                        _eligible(
                            global_history,
                            self.config.http.delay_seconds,
                            global_rules,
                            self.config.http.user_agent,
                            now,
                        ),
                    )
                wait = eligible - now
                if wait <= 0:
                    self.state.origins[host] = _started(
                        global_history, now, global_rules, self.config.http.user_agent
                    )
                    local_state.origins[host] = _started(
                        local_history,
                        now,
                        local_rules,
                        route.config.http.user_agent,
                    )
                    self._save()
                    return
            if not reported and wait > 0.05:
                reason = (
                    "robots timing"
                    if eligible > configured_eligible
                    else "configured interval"
                )
                self.report(
                    f"{proxy_id}: waiting {wait:.1f}s before requesting {host} ({reason})"
                )
                reported = True
            self.cancelled.wait(min(0.25, max(0, wait)))

    def defer(
        self,
        proxy_id: str,
        url: str,
        *,
        until: float | None,
        pause: PauseState | None,
        shared: bool,
    ) -> None:
        with self.guard:
            host = origin(url)
            state = self.state.transports[proxy_id]
            if until is not None:
                history = state.origins.get(host, OriginThrottle())
                state.origins[host] = history.model_copy(
                    update={"cooldown_until": max(history.cooldown_until, until)}
                )
                if shared:
                    history = self.state.origins.get(host, OriginThrottle())
                    self.state.origins[host] = history.model_copy(
                        update={"cooldown_until": max(history.cooldown_until, until)}
                    )
            if pause is not None:
                self.state.transports[proxy_id] = state.model_copy(
                    update={"pause": pause}
                )
            self._save()

    def resume(self, proxy_ids: tuple[str, ...] | None = None) -> None:
        selected = tuple(self.routes) if proxy_ids is None else proxy_ids
        if any(proxy_id not in self.routes for proxy_id in selected):
            raise VinfError("Cannot resume an unconfigured proxy.")
        with self.guard:
            changed = False
            for proxy_id in selected:
                state = self.state.transports[proxy_id]
                if state.pause or state.disabled:
                    self.state.transports[proxy_id] = state.model_copy(
                        update={"pause": None, "disabled": None}
                    )
                    changed = True
            if changed:
                self._save()

    def disable(self, proxy_id: str, reason: str) -> None:
        with self.guard:
            state = self.state.transports[proxy_id]
            self.state.transports[proxy_id] = state.model_copy(
                update={"disabled": reason}
            )
            self._save()
