"""Job-board clickstream: external, internal (HT3), and bot sessions, one day at a time.

SPEC §6.5 sets the volume model and the HT3 multipliers; ADR-0007 fixes session structure,
pacing, referrers, bots, and envelope details. Session-level and view-level randomness is drawn
in vectors for the whole day; events are then built session by session, sorted by `event_ts`,
and written to a sink. Every `apply_submit` also becomes a `Submission` for the ATS (T1.6).
"""

from __future__ import annotations

import math
import uuid
from array import array
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from functools import partial
from typing import Any, Literal
from zoneinfo import ZoneInfo

import numpy as np
import numpy.typing as npt

from hirestream.generator.calendar import CalendarEvent
from hirestream.generator.candidates import CandidateRegistry, Channel, Submission
from hirestream.generator.config import DayOfWeek, GeneratorConfig
from hirestream.generator.events import EventSink, StreamEvent, iso_utc_ms
from hirestream.generator.requisitions import Requisition, Requisitions, base_title
from hirestream.generator.workforce import Workforce

SOURCE = "jobboard-web"
PRODUCER_VERSIONS = {1: "3.2.0", 2: "3.3.0"}
DAY_MS = 86_400_000
_WEEKDAYS: tuple[DayOfWeek, ...] = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_ID_MULTIPLIER = 0x9E3779B97F4A7C15  # odd, so n -> n * M + salt is a bijection mod 2**48
_ID_SPACE = 1 << 48

_WEBKIT = "AppleWebKit/537.36 (KHTML, like Gecko)"
_SAFARI = "AppleWebKit/605.1.15 (KHTML, like Gecko)"
USER_AGENTS: dict[str, tuple[str, ...]] = {
    "desktop": (
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) {_WEBKIT} Chrome/128.0.0.0 Safari/537.36",
        f"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) {_SAFARI} Version/17.6 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
        f"Mozilla/5.0 (X11; Linux x86_64) {_WEBKIT} Chrome/128.0.0.0 Safari/537.36",
    ),
    "mobile": (
        f"Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) {_SAFARI} Version/17.6"
        " Mobile/15E148 Safari/604.1",
        f"Mozilla/5.0 (Linux; Android 14; Pixel 8) {_WEBKIT} Chrome/128.0.0.0 Mobile Safari/537.36",
        f"Mozilla/5.0 (Linux; Android 14; SM-S921B) {_WEBKIT} Chrome/128.0.0.0"
        " Mobile Safari/537.36",
    ),
    "tablet": (
        f"Mozilla/5.0 (iPad; CPU OS 17_6 like Mac OS X) {_SAFARI} Version/17.6 Mobile/15E148"
        " Safari/604.1",
        f"Mozilla/5.0 (Linux; Android 14; SM-X710) {_WEBKIT} Chrome/128.0.0.0 Safari/537.36",
    ),
}
BOT_USER_AGENTS = (
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "Mozilla/5.0 (compatible; AhrefsBot/7.0; +http://ahrefs.com/robot/)",
    "python-requests/2.32.3",
    "Scrapy/2.11.2 (+https://scrapy.org)",
)

SessionKind = Literal["external", "internal", "bot"]
Step = tuple[str, dict[str, Any], bool]  # (event_type, payload, waits for a pause first)


@dataclass
class JobBoardTruth:
    """What actually happened, before any chaos (SPEC §6.1): the basis for ground truth."""

    sessions: Counter[str] = field(default_factory=Counter)
    events: Counter[str] = field(default_factory=Counter)  # "human" / "bot"
    applications: Counter[str] = field(default_factory=Counter)  # by channel
    ht3_applications: Counter[tuple[str, str]] = field(default_factory=Counter)  # (month, group)
    ht3_employee_days: Counter[tuple[str, str]] = field(default_factory=Counter)

    def summary(self) -> dict[str, int]:
        return {
            "events": sum(self.events.values()),
            "bot_events": self.events["bot"],
            "sessions": sum(self.sessions.values()),
            "career_site": self.applications["career_site"],
            "internal": self.applications["internal"],
        }


@dataclass(slots=True)
class _Session:
    number: int
    visitor: int
    kind: SessionKind
    start_ms: int
    device: str
    user_agent: str
    referrer: str
    steps: list[Step]
    employee_id: str | None = None
    long_in_role: bool = False


def view_weights(
    reqs: Sequence[Requisition], day: date, config: GeneratorConfig
) -> npt.NDArray[np.float64]:
    """Relative views per open req today: popularity x season x weekday x posting-age decay.

    Evergreen reqs don't decay (SPEC §6.5). Multiply by `base_daily_views_per_open_req` for the
    expected external views.
    """
    jb = config.jobboard
    factor = jb.seasonality_by_month[day.month] * jb.day_of_week[_WEEKDAYS[day.weekday()]]
    ages = np.array([(day - r.opened_on).days for r in reqs], dtype=float)
    decay = np.power(0.5, ages / jb.posting_age_half_life_days)
    evergreen = np.array([r.is_evergreen for r in reqs], dtype=bool)
    popularity = np.array([r.popularity for r in reqs], dtype=float)
    return popularity * factor * np.where(evergreen, 1.0, decay)


def diurnal_start_ms(
    rng: np.random.Generator,
    midnight_ms: npt.NDArray[np.int64],
    peaks: Sequence[float],
    spread_hours: float,
) -> npt.NDArray[np.int64]:
    """Session starts from a mixture of normals around local peak hours, one per local midnight."""
    n = len(midnight_ms)
    which = rng.integers(0, len(peaks), size=n)
    hours = (np.asarray(peaks, dtype=float)[which] + rng.normal(0.0, spread_hours, size=n)) % 24.0
    return midnight_ms + np.floor(hours * 3_600_000).astype(np.int64)


class JobBoard:
    def __init__(
        self,
        config: GeneratorConfig,
        calendar: Mapping[str, CalendarEvent],
        rng: np.random.Generator,
        workforce: Workforce,
        requisitions: Requisitions,
        candidates: CandidateRegistry,
    ) -> None:
        self.truth = JobBoardTruth()
        self.expected_views: dict[str, float] = {}  # today's external views per req (for T1.6)
        self._config = config
        self._jb = config.jobboard
        self._rng = rng
        self._wf = workforce
        self._rq = requisitions
        self._candidates = candidates
        self._v2_from = calendar["chaos.schema_v2.jobboard"].start
        locations = config.org_model.locations
        self._cities = [loc.city for loc in locations]
        self._zones = [ZoneInfo(loc.tz) for loc in locations]
        self._city_index = {city: k for k, city in enumerate(self._cities)}
        self._loc_p = _normalised([loc.weight for loc in locations])
        self._devices = list(self._jb.device_mix_v2)
        self._device_p = _normalised(list(self._jb.device_mix_v2.values()))
        self._referrers = list(self._jb.session.referrer_mix)
        self._referrer_p = _normalised(list(self._jb.session.referrer_mix.values()))
        ext = self._jb.external
        p, cap = ext.views_per_session_geometric_p, ext.max_views_per_session
        self._mean_size = (1 - (1 - p) ** cap) / p  # E[min(Geometric(p), cap)]

        self._salt = int(rng.integers(0, _ID_SPACE))
        self._next_visitor = 0
        self._next_session = 0
        self._visitors = array("q")  # external visitors: visitor number, city, device, agent
        self._visitor_city = array("b")
        self._visitor_device = array("b")
        self._visitor_agent = array("b")
        bots = self._jb.bots
        known = rng.random(bots.distinct_visitor_pool) < bots.known_bot_user_agent_share
        self._bots = [
            (
                self._new_visitor_number(),
                BOT_USER_AGENTS[k % len(BOT_USER_AGENTS)]
                if is_known
                else _agents("desktop")[k % len(_agents("desktop"))],
            )
            for k, is_known in enumerate(known)
        ]
        self._employee_visitor: dict[str, tuple[int, str, str]] = {}  # id -> visitor, device, UA
        self._midnight_cache: dict[tuple[date, int], int] = {}

    # ------------------------------------------------------------------ public API

    def step(self, day: date, sink: EventSink) -> list[Submission]:
        """Generate the day's sessions, write their events to `sink`, return the applications."""
        reqs = self._rq.open_reqs()
        weights = view_weights(reqs, day, self._config) if reqs else np.zeros(0)
        public = np.array([not r.is_internal_only for r in reqs], dtype=bool)
        lam = weights[public] * self._jb.external.base_daily_views_per_open_req
        public_reqs = [r for r, is_public in zip(reqs, public, strict=True) if is_public]
        self.expected_views = {r.req_id: float(x) for r, x in zip(public_reqs, lam, strict=True)}

        sessions = self._external(day, public_reqs, lam)
        humans = len(sessions)
        sessions += self._bot_sessions(day, public_reqs, humans)
        sessions += self._internal(day, reqs, weights)
        events, submissions = self._emit(day, sessions)
        sink.write(events)
        return submissions

    # ------------------------------------------------------------------ external sessions

    def _external(
        self, day: date, reqs: list[Requisition], lam: npt.NDArray[np.float64]
    ) -> list[_Session]:
        rng, ext, sess = self._rng, self._jb.external, self._jb.session
        total = float(lam.sum())
        n = int(rng.poisson(total / self._mean_size)) if total > 0 else 0
        if n == 0:
            return []
        sizes = np.minimum(
            rng.geometric(ext.views_per_session_geometric_p, n), ext.max_views_per_session
        )
        views = rng.choice(len(reqs), size=int(sizes.sum()), p=lam / total)

        returning = rng.random(n) < ext.returning_visitor_share
        if len(self._visitors) == 0:
            returning[:] = False
        picks = rng.integers(0, max(len(self._visitors), 1), size=int(returning.sum()))
        fresh = n - int(returning.sum())
        new_city = rng.choice(len(self._cities), size=fresh, p=self._loc_p)
        new_device = rng.choice(len(self._devices), size=fresh, p=self._device_p)
        new_agent = rng.integers(0, 1 << 16, size=fresh)
        referrers = rng.choice(len(self._referrers), size=n, p=self._referrer_p)
        search = rng.random(n) < ext.p_search_before_view
        filter_city = rng.random(n) < sess.filter_location_share
        filter_family = rng.random(n) < sess.filter_role_family_share
        k = len(views)
        positions = rng.geometric(sess.results_position_geometric_p, k)
        saves = rng.random(k) < ext.p_save_given_view
        starts = rng.random(k) < ext.p_apply_start_given_view
        submits = rng.random(k) < ext.p_apply_submit_given_start

        visitor = np.empty(n, dtype=np.int64)
        city = np.empty(n, dtype=np.int64)
        device = np.empty(n, dtype=np.int64)
        agent = np.empty(n, dtype=np.int64)
        r_i = f_i = 0
        for i in range(n):  # visitors are registered in session order
            if returning[i]:
                slot = int(picks[r_i])
                r_i += 1
                visitor[i] = self._visitors[slot]
                city[i], device[i] = self._visitor_city[slot], self._visitor_device[slot]
                agent[i] = self._visitor_agent[slot]
            else:
                visitor[i] = self._new_visitor_number()
                city[i], device[i] = int(new_city[f_i]), int(new_device[f_i])
                agent[i] = int(new_agent[f_i]) % len(_agents(self._devices[int(device[i])]))
                self._visitors.append(int(visitor[i]))
                self._visitor_city.append(int(city[i]))
                self._visitor_device.append(int(device[i]))
                self._visitor_agent.append(int(agent[i]))
                f_i += 1
        midnights = np.array([self._midnight(day, int(c)) for c in city], dtype=np.int64)
        start_ms = diurnal_start_ms(
            rng, midnights, ext.diurnal_peaks_local_hour, sess.diurnal_spread_hours
        )

        counts = _match_counts(reqs)
        sessions, offset = [], 0
        for i in range(n):
            chosen = [reqs[int(v)] for v in views[offset : offset + int(sizes[i])]]
            span = slice(offset, offset + int(sizes[i]))
            offset += int(sizes[i])
            steps = self._human_steps(
                day, chosen, bool(search[i]), bool(filter_city[i]), bool(filter_family[i]),
                positions[span], saves[span], starts[span], submits[span], counts,
                applicant=partial(
                    self._candidates.external,
                    self._rng, int(visitor[i]), self._cities[int(city[i])], day,
                ),
            )  # fmt: skip
            device_name = self._devices[int(device[i])]
            sessions.append(
                _Session(
                    number=self._new_session_number(),
                    visitor=int(visitor[i]),
                    kind="external",
                    start_ms=int(start_ms[i]),
                    device=device_name,
                    user_agent=_agents(device_name)[int(agent[i])],
                    referrer=self._referrers[int(referrers[i])],
                    steps=steps,
                )
            )
        return sessions

    def _human_steps(
        self,
        day: date,
        chosen: list[Requisition],
        search: bool,
        filter_city: bool,
        filter_family: bool,
        positions: npt.NDArray[np.int64],
        saves: npt.NDArray[np.bool_],
        starts: npt.NDArray[np.bool_],
        submits: npt.NDArray[np.bool_],
        counts: Counter[tuple[str | None, str | None]],
        applicant: Callable[[], str],
    ) -> list[Step]:
        """One human session's events (ADR-0007 §3). Filters come from the first viewed req."""
        steps: list[Step] = []
        results = 0
        if search:
            first = chosen[0]
            city = first.location_city if filter_city else None
            family = first.role_family if filter_family else None
            results = counts[(family, city)]
            query = base_title(first.role_family).lower() + (f" {city.lower()}" if city else "")
            steps += [
                ("page_view", {"page_type": "home", "req_id": None}, False),
                ("job_search", {"query_text": query, "filter_location": city,
                                "filter_role_family": family, "results_count": results}, True),
                ("page_view", {"page_type": "search_results", "req_id": None}, False),
            ]  # fmt: skip
        applied: set[str] = set()
        for j, req in enumerate(chosen):
            rid = req.req_id
            position = min(int(positions[j]), results) if search else None
            steps.append(("page_view", {"page_type": "job_detail", "req_id": rid}, bool(steps)))
            steps.append(("job_view", {"req_id": rid, "position_in_results": position}, False))
            if saves[j]:
                steps.append(("job_save", {"req_id": rid}, True))
            if starts[j] and rid not in applied:
                steps.append(("page_view", {"page_type": "apply_form", "req_id": rid}, True))
                steps.append(("apply_start", {"req_id": rid}, False))
                if submits[j]:
                    applied.add(rid)
                    payload = {
                        "req_id": rid,
                        "application_id": self._candidates.new_application_id(),
                        "candidate_id": applicant(),
                    }
                    steps.append(("apply_submit", payload, True))
                    steps.append(("page_view", {"page_type": "confirmation", "req_id": rid}, False))
        return steps

    # ------------------------------------------------------------------ bots

    def _bot_sessions(self, day: date, reqs: list[Requisition], humans: int) -> list[_Session]:
        bots, rng = self._jb.bots, self._rng
        if not reqs:
            return []
        n = int(rng.poisson(humans * bots.session_share / (1 - bots.session_share)))
        day_start = _epoch_ms(day)
        sessions = []
        lo, hi = bots.job_views_per_session
        for _ in range(n):
            visitor, agent = self._bots[int(rng.integers(len(self._bots)))]
            crawl = rng.integers(0, len(reqs), size=int(rng.integers(lo, hi + 1)))
            steps: list[Step] = []
            for j in crawl:
                rid = reqs[int(j)].req_id
                steps.append(("page_view", {"page_type": "job_detail", "req_id": rid}, bool(steps)))
                steps.append(("job_view", {"req_id": rid, "position_in_results": None}, False))
            sessions.append(
                _Session(
                    number=self._new_session_number(),
                    visitor=visitor,
                    kind="bot",
                    start_ms=day_start + int(rng.integers(0, DAY_MS)),
                    device="desktop",
                    user_agent=agent,
                    referrer="direct",
                    steps=steps,
                )
            )
        return sessions

    # ------------------------------------------------------------------ internal sessions (HT3)

    def _internal(
        self, day: date, reqs: list[Requisition], weights: npt.NDArray[np.float64]
    ) -> list[_Session]:
        rng, internal = self._rng, self._jb.internal
        ht3 = self._config.workforce.mobility_propensity
        active = self._wf.active_mask()
        long = self._wf.days_in_role(day) >= ht3.tenure_in_role_threshold_days
        month = f"{day:%Y-%m}"
        self.truth.ht3_employee_days[(month, "long")] += int((active & long).sum())
        self.truth.ht3_employee_days[(month, "short")] += int((active & ~long).sum())
        p = internal.p_employee_browses_per_day * np.where(long, ht3.browse_multiplier, 1.0)
        browsing = np.flatnonzero((rng.random(len(active)) < p) & active)
        if not reqs or len(browsing) == 0:
            return []

        teams = np.array([r.team for r in reqs])
        counts = _match_counts(reqs)
        ext, sess = self._jb.external, self._jb.session
        sessions = []
        for i in (int(x) for x in browsing):
            emp = self._wf.world.employees[i]
            w = np.where(teams == emp.team, 0.0, weights)
            if w.sum() <= 0:
                continue
            size = min(
                int(rng.geometric(ext.views_per_session_geometric_p)), ext.max_views_per_session
            )
            chosen = [reqs[int(v)] for v in rng.choice(len(reqs), size=size, p=w / w.sum())]
            is_long = bool(long[i])
            p_start = internal.p_apply_start_given_view * (ht3.apply_multiplier if is_long else 1.0)
            search = bool(rng.random() < ext.p_search_before_view)
            filter_city = bool(rng.random() < sess.filter_location_share)
            filter_family = bool(rng.random() < sess.filter_role_family_share)
            steps = self._human_steps(
                day, chosen, search, filter_city, filter_family,
                rng.geometric(sess.results_position_geometric_p, size),
                rng.random(size) < ext.p_save_given_view,
                rng.random(size) < p_start,
                rng.random(size) < internal.p_apply_submit_given_start,
                counts,
                applicant=partial(
                    self._candidates.internal, emp.employee_id, emp.location_city, day
                ),
            )  # fmt: skip
            visitor, device, agent = self._employee_identity(emp.employee_id)
            city = self._city_index[emp.location_city]
            start = diurnal_start_ms(
                rng,
                np.array([self._midnight(day, city)], dtype=np.int64),
                ext.diurnal_peaks_local_hour,
                sess.diurnal_spread_hours,
            )
            sessions.append(
                _Session(
                    number=self._new_session_number(),
                    visitor=visitor,
                    kind="internal",
                    start_ms=int(start[0]),
                    device=device,
                    user_agent=agent,
                    referrer="internal_portal",
                    steps=steps,
                    employee_id=emp.employee_id,
                    long_in_role=is_long,
                )
            )
        return sessions

    def _employee_identity(self, employee_id: str) -> tuple[int, str, str]:
        known = self._employee_visitor.get(employee_id)
        if known is None:
            device = self._devices[int(self._rng.choice(len(self._devices), p=self._device_p))]
            options = _agents(device)
            known = (
                self._new_visitor_number(),
                device,
                options[int(self._rng.integers(len(options)))],
            )
            self._employee_visitor[employee_id] = known
        return known

    # ------------------------------------------------------------------ envelopes

    def _emit(
        self, day: date, sessions: list[_Session]
    ) -> tuple[list[StreamEvent], list[Submission]]:
        rng, sess = self._rng, self._jb.session
        version = 2 if day >= self._v2_from else 1
        producer = PRODUCER_VERSIONS[version]
        human_waits = sum(w for s in sessions if s.kind != "bot" for _, _, w in s.steps)
        bot_waits = sum(w for s in sessions if s.kind == "bot" for _, _, w in s.steps)
        total = sum(len(s.steps) for s in sessions)
        pause = sess.seconds_between_events
        think = rng.lognormal(math.log(pause.median), pause.sigma, size=human_waits)
        lo, hi = self._jb.bots.seconds_between_views
        bot_gaps = rng.uniform(lo, hi, size=bot_waits)
        delays = rng.integers(0, 5001, size=total)
        ids = rng.integers(
            0, np.iinfo(np.uint64).max, size=(total, 2), dtype=np.uint64, endpoint=True
        )

        events: list[StreamEvent] = []
        submissions: list[Submission] = []
        h = b = k = 0
        for s in sessions:
            session_id = self._session_id(s.number)
            context: dict[str, Any] = {
                "session_id": session_id,
                "visitor_id": self._visitor_id(s.visitor),
                "employee_id": s.employee_id,
                "user_agent": s.user_agent,
                "referrer_type": s.referrer,
            }
            if version == 2:
                context["device_type"] = s.device
            human = s.kind != "bot"
            self.truth.sessions[s.kind] += 1
            self.truth.events["human" if human else "bot"] += len(s.steps)
            ts = s.start_ms
            for event_type, payload, waits in s.steps:
                if waits:
                    if human:
                        ts += int(think[h] * 1000)
                        h += 1
                    else:
                        ts += int(bot_gaps[b] * 1000)
                        b += 1
                sent = ts + int(delays[k])
                event_id = str(uuid.UUID(int=(int(ids[k, 0]) << 64) | int(ids[k, 1]), version=4))
                k += 1
                body = {
                    "event_id": event_id,
                    "event_type": event_type,
                    "schema_version": version,
                    "source": SOURCE,
                    "producer_version": producer,
                    "event_ts": ts,
                    "sent_ts": iso_utc_ms(sent),
                    "context": dict(context),
                    "payload": payload,
                }
                events.append(StreamEvent(SOURCE, event_type, ts, sent, session_id, body))
                if event_type == "apply_submit":
                    channel: Channel = "internal" if s.employee_id else "career_site"
                    submissions.append(
                        Submission(
                            payload["application_id"], payload["candidate_id"], payload["req_id"],
                            channel, ts, s.employee_id,
                        )
                    )  # fmt: skip
                    self.truth.applications[channel] += 1
                    if s.employee_id:
                        group = "long" if s.long_in_role else "short"
                        self.truth.ht3_applications[(f"{day:%Y-%m}", group)] += 1
        events.sort(key=lambda e: e.event_ts_ms)  # stable: ties keep session order
        return events, submissions

    # ------------------------------------------------------------------ ids and time

    def _new_visitor_number(self) -> int:
        self._next_visitor += 1
        return self._next_visitor

    def _new_session_number(self) -> int:
        self._next_session += 1
        return self._next_session

    def _visitor_id(self, number: int) -> str:
        return f"v-{(number * _ID_MULTIPLIER + self._salt) % _ID_SPACE:012x}"

    def _session_id(self, number: int) -> str:
        return f"s-{(number * _ID_MULTIPLIER + self._salt) % _ID_SPACE:012x}"

    def _midnight(self, day: date, city: int) -> int:
        key = (day, city)
        if key not in self._midnight_cache:
            local = datetime(day.year, day.month, day.day, tzinfo=self._zones[city])
            self._midnight_cache[key] = int(local.timestamp() * 1000)
        return self._midnight_cache[key]


def _agents(device: str) -> tuple[str, ...]:
    return USER_AGENTS.get(device, USER_AGENTS["desktop"])


def _normalised(weights: Sequence[float]) -> npt.NDArray[np.float64]:
    w = np.asarray(weights, dtype=np.float64)
    return np.asarray(w / w.sum(), dtype=np.float64)


def _epoch_ms(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp() * 1000)


def _match_counts(reqs: Sequence[Requisition]) -> Counter[tuple[str | None, str | None]]:
    """Open reqs matching every (role family filter, city filter) pair, including 'no filter'."""
    counts: Counter[tuple[str | None, str | None]] = Counter()
    for req in reqs:
        for family in (req.role_family, None):
            for city in (req.location_city, None):
                counts[(family, city)] += 1
    return counts
