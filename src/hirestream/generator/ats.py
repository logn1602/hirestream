"""The ATS: applications move through stages to offers and hires (SPEC §6.6, ADR-0008).

Each day the ATS takes the job board's submissions and draws referral, sourced, and agency
applications, then advances applications whose decision is due. The decision for a stage is
drawn when the application enters it (advance, withdraw, reject), together with how long it takes.
Accepted offers take a requisition seat; on the start date the candidate joins the workforce
(external) or changes job (internal), unless the offer turns into a no-start.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

import numpy as np

from hirestream.generator.candidates import Candidate, CandidateRegistry, Channel, Submission
from hirestream.generator.config import GeneratorConfig, Lognormal
from hirestream.generator.people import EmailAllocator, PersonNamer, fictional_phone
from hirestream.generator.requisitions import ACTIVE, ReqEvent, Requisition, Requisitions
from hirestream.generator.workforce import Workforce, WorkforceEvent

STAGES = ("applied", "recruiter_screen", "phone_screen", "onsite", "offer")
BEFORE_ONSITE = frozenset(STAGES[:3])
DIRECT_CHANNELS: tuple[Channel, ...] = ("referral", "sourced", "agency")
HT4_BUCKETS = ((30, "<=30"), (45, "31-45"), (60, "46-60"))  # anything later is ">60"

Status = Literal["active", "rejected", "withdrawn", "hired", "offer_declined", "no_start"]
Outcome = Literal["advance", "withdraw", "reject"]
OfferStatus = Literal["extended", "accepted", "declined", "rescinded"]


@dataclass(slots=True)
class Application:
    application_id: str
    candidate_id: str
    req_id: str
    channel: Channel
    applied_ms: int
    applied_on: date
    employee_id: str | None
    stage: str = "applied"
    status: Status = "active"
    status_reason: str | None = None
    updated_ms: int = 0
    outcome: Outcome | None = None  # the decision already drawn for the current stage
    due: date | None = None  # when that decision happens


@dataclass(frozen=True, slots=True)
class StageChange:
    """One row of `application_stage_changes` (append-only, SPEC §7.4)."""

    change_id: int
    application_id: str
    from_stage: str | None
    to_stage: str
    from_status: str | None
    to_status: str
    reason: str
    changed_ms: int
    changed_by: str


@dataclass(slots=True)
class Offer:
    offer_id: str
    application_id: str
    extended_ms: int
    decide_on: date
    accepted: bool  # drawn when the offer is extended (HT4)
    days_to_offer: int
    status: OfferStatus = "extended"
    decided_ms: int | None = None
    start_date: date | None = None
    updated_ms: int = 0
    no_start: bool = False


class InterviewTiming(Protocol):
    """Days an interview stage takes; the scheduling engine (T1.7) replaces the stand-in."""

    def stage_days(self, rng: np.random.Generator, stage: str) -> int: ...


class LeadTimeInterviews:
    """Stand-in until T1.7 (ADR-0008 §2): lead time + one feedback latency + decision delay."""

    def __init__(self, config: GeneratorConfig) -> None:
        sched = config.scheduling
        self._lead = {
            "phone_screen": sched.phone_screen.lead_days,
            "onsite": sched.onsite.lead_days,
        }
        self._feedback = sched.feedback.latency_hours
        self._decision = config.ats.interview_stage_decision_delay_days
        self._cap = config.ats.feedback_wait_cap_days

    def stage_days(self, rng: np.random.Generator, stage: str) -> int:
        lead = _lognormal(rng, self._lead[stage])
        feedback = min(_lognormal(rng, self._feedback) / 24, self._cap)
        return max(1, round(lead + feedback + _lognormal(rng, self._decision)))


@dataclass
class ATSTruth:
    """What actually happened (SPEC §6.10 ground truth): counts as they are created."""

    applications: Counter[str] = field(default_factory=Counter)  # by channel
    first_gate: Counter[tuple[str, str]] = field(default_factory=Counter)  # (channel, outcome)
    offers: Counter[tuple[str, str, bool]] = field(
        default_factory=Counter
    )  # (month, channel, internal)
    accepted: Counter[tuple[str, str, bool]] = field(default_factory=Counter)
    declined: Counter[tuple[str, str, bool]] = field(default_factory=Counter)
    hires: Counter[tuple[str, str, bool]] = field(default_factory=Counter)  # by start month
    ht4: Counter[tuple[str, bool]] = field(
        default_factory=Counter
    )  # (days-to-offer bucket, accepted)
    no_starts: int = 0

    def summary(self) -> dict[str, int]:
        hires_internal = sum(n for (_, _, internal), n in self.hires.items() if internal)
        return {
            "applications": sum(self.applications.values()),
            "career_site": self.applications["career_site"],
            "internal": self.applications["internal"],
            "direct": sum(self.applications[c] for c in DIRECT_CHANNELS),
            "offers": sum(self.offers.values()),
            "hires": sum(self.hires.values()),
            "hires_internal": hires_internal,
            "no_starts": self.no_starts,
        }


def accept_probability(config: GeneratorConfig, days_to_offer: int, internal: bool) -> float:
    """HT4: acceptance falls `per_day` for every day time-to-offer exceeds `threshold_days`."""
    offer = config.ats.offer
    base = (
        offer.base_accept_probability.internal
        if internal
        else offer.base_accept_probability.external
    )
    late = max(0, days_to_offer - offer.accept_decay.threshold_days)
    return max(offer.accept_probability_floor, base - offer.accept_decay.per_day * late)


def ht4_bucket(days_to_offer: int) -> str:
    for limit, label in HT4_BUCKETS:
        if days_to_offer <= limit:
            return label
    return ">60"


class ATS:
    def __init__(
        self,
        config: GeneratorConfig,
        rng: np.random.Generator,
        faker_seed: int,
        workforce: Workforce,
        requisitions: Requisitions,
        candidates: CandidateRegistry,
        interviews: InterviewTiming | None = None,
    ) -> None:
        self.applications: dict[str, Application] = {}
        self.changes: list[StageChange] = []
        self.offers: dict[str, Offer] = {}
        self.truth = ATSTruth()
        self._config = config
        self._ats = config.ats
        self._rng = rng
        self._wf = workforce
        self._rq = requisitions
        self._candidates = candidates
        self._interviews = interviews or LeadTimeInterviews(config)
        locations = config.org_model.locations
        self._country = {loc.city: loc.country for loc in locations}
        self._zone = {loc.city: ZoneInfo(loc.tz) for loc in locations}
        self._namer = PersonNamer(config.meta.faker_locale_by_country, faker_seed)
        self._emails = {d: EmailAllocator(d) for d in config.meta.external_email_domains}
        self._domains = list(config.meta.external_email_domains)
        lo, hi = config.scheduling.business_hours_local
        self._hours = (lo * 3_600_000, hi * 3_600_000)
        self._midnights: dict[tuple[date, str], int] = {}
        self._decisions: dict[int, list[str]] = {}  # day -> applications with a decision due
        self._closures: dict[int, list[tuple[str, str]]] = {}  # day -> (application, reason)
        self._offer_days: dict[int, list[str]] = {}  # day -> offers to decide
        self._starts: dict[int, list[str]] = {}  # day -> accepted offers starting
        self._flips: dict[int, list[str]] = {}  # day -> accepted offers becoming no-starts
        self._by_req: dict[str, set[str]] = {}  # active applications per req
        self._by_employee: dict[str, set[str]] = {}  # active internal applications per employee
        self._pending_offers: Counter[str] = Counter()  # extended, undecided offers per req
        self._next_offer = 1

    # ------------------------------------------------------------------ public API

    def step(
        self,
        day: date,
        submissions: Sequence[Submission],
        expected_views: Mapping[str, float],
        workforce_events: Sequence[WorkforceEvent],
        req_events: Sequence[ReqEvent],
    ) -> None:
        """Advance one day, after the workforce, requisitions, and job board."""
        for event in workforce_events:
            if event.kind == "termination":
                self._left_company(day, event.employee_id)
        for req_event in req_events:
            if req_event.kind in ("cancelled", "expired"):
                self._req_closed(day, req_event.req_id, "req_cancelled")
        for sub in submissions:
            self._apply(
                day,
                sub.application_id,
                sub.candidate_id,
                sub.req_id,
                sub.channel,
                sub.applied_ts_ms,
                sub.employee_id,
            )
        self._direct_applications(day, expected_views)
        self._fill_in_candidates()
        for app_id in self._decisions.pop(day.toordinal(), []):
            self._decide(day, self.applications[app_id])
        for app_id, reason in self._closures.pop(day.toordinal(), []):
            app = self.applications[app_id]
            if app.status == "active" and app.stage in BEFORE_ONSITE:
                self._close(app, "rejected", reason, self._at(day, app), "system")
        for offer_id in self._offer_days.pop(day.toordinal(), []):
            self._decide_offer(day, self.offers[offer_id])
        for offer_id in self._starts.pop(day.toordinal(), []):
            self._start(day, self.offers[offer_id])
        for offer_id in self._flips.pop(day.toordinal(), []):
            self._flip_to_no_start(day, self.offers[offer_id], "no_start")

    # ------------------------------------------------------------------ intake

    def _apply(
        self,
        day: date,
        app_id: str,
        candidate_id: str,
        req_id: str,
        channel: Channel,
        applied_ms: int,
        employee_id: str | None,
    ) -> None:
        app = Application(app_id, candidate_id, req_id, channel, applied_ms, day, employee_id)
        self.applications[app_id] = app
        self._by_req.setdefault(req_id, set()).add(app_id)
        if employee_id is not None:
            self._by_employee.setdefault(employee_id, set()).add(app_id)
        self.truth.applications[channel] += 1
        self._enter(day, app, "applied", applied_ms, "candidate", "submitted")

    def _direct_applications(self, day: date, expected_views: Mapping[str, float]) -> None:
        """Referral, sourced, and agency applications per req-day, from expected views (§6.6)."""
        if not expected_views:
            return
        req_ids = list(expected_views)
        rates = np.array(
            [self._ats.direct_apps_per_1000_external_views[c] for c in DIRECT_CHANNELS]
        )
        views = np.array([expected_views[r] for r in req_ids])
        counts = self._rng.poisson(np.outer(views, rates) / 1000).tolist()
        for req_id, per_channel in zip(req_ids, counts, strict=True):
            req = self._rq.reqs[req_id]
            for channel, n in zip(DIRECT_CHANNELS, per_channel, strict=True):
                for _ in range(n):
                    candidate_id = self._candidates.direct(self._rng, req.location_city, day)
                    app_id = self._candidates.new_application_id()
                    self._apply(
                        day,
                        app_id,
                        candidate_id,
                        req_id,
                        channel,
                        self._at_city(day, req.location_city),
                        None,
                    )

    def _fill_in_candidates(self) -> None:
        """Names, emails, and phones for today's new candidates, in candidate-id order."""
        for candidate_id in self._candidates.unnamed:
            cand = self._candidates.candidates[candidate_id]
            if cand.employee_id is not None:
                emp = self._wf.employee(cand.employee_id)
                cand.first_name, cand.last_name, cand.email = (
                    emp.first_name,
                    emp.last_name,
                    emp.work_email,
                )
                cand.location_city = emp.location_city
            else:
                country = self._country[cand.location_city]
                cand.first_name, cand.last_name = self._namer.name(country)
                domain = self._domains[int(self._rng.integers(len(self._domains)))]
                cand.email = self._emails[domain].allocate(cand.first_name, cand.last_name)
            cand.location_country = self._country[cand.location_city]
            cand.phone = fictional_phone(self._rng, cand.location_country)
            first = self._first_application_ms(cand)
            cand.created_ms = first
        self._candidates.unnamed.clear()

    def _first_application_ms(self, cand: Candidate) -> int:
        midnight = self._midnight(cand.first_applied_on, cand.location_city)
        return midnight + self._hours[0]

    # ------------------------------------------------------------------ the stage machine

    def _enter(
        self, day: date, app: Application, stage: str, ms: int, by: str, reason: str
    ) -> None:
        self._record(
            app,
            app.stage if reason != "submitted" else None,
            stage,
            app.status if reason != "submitted" else None,
            "active",
            reason,
            ms,
            by,
        )
        app.stage = stage
        if stage == "offer":
            self._extend_offer(day, app, ms)
            return
        gates = self._ats.internal if app.employee_id else self._ats.external
        p_advance = getattr(gates.p_advance, stage)
        p_withdraw = getattr(gates.p_withdraw, stage)
        if stage == "applied":  # HT2: some channels pass the first screen more often
            p_advance = min(0.95, p_advance * self._ats.first_gate_channel_multiplier[app.channel])
        u = self._rng.random()
        outcome: Outcome = (
            "advance" if u < p_advance else "withdraw" if u < p_advance + p_withdraw else "reject"
        )
        if stage == "applied":
            self.truth.first_gate[(app.channel, outcome)] += 1
        if stage in ("phone_screen", "onsite"):
            days = self._interviews.stage_days(self._rng, stage)
        else:
            delay = _lognormal(self._rng, getattr(self._ats.stage_delay_days, stage))
            if outcome == "reject":
                delay *= self._ats.rejection_delay_multiplier
            days = max(1, round(delay))
        app.outcome, app.due = outcome, day + timedelta(days=days)
        self._decisions.setdefault(app.due.toordinal(), []).append(app.application_id)

    def _decide(self, day: date, app: Application) -> None:
        if app.status != "active" or app.due != day:
            return  # closed in the meantime
        req = self._rq.reqs[app.req_id]
        ms = self._at(day, app)
        by = self._decider(req, app.stage)
        if app.outcome == "advance":
            self._enter(day, app, STAGES[STAGES.index(app.stage) + 1], ms, by, "advanced")
        elif app.outcome == "withdraw":
            self._close(app, "withdrawn", "candidate_withdrew", ms, "candidate")
        else:
            self._close(app, "rejected", "not_selected", ms, by)

    def _close(self, app: Application, status: Status, reason: str, ms: int, by: str) -> None:
        self._record(app, app.stage, app.stage, app.status, status, reason, ms, by)
        app.status, app.status_reason, app.due = status, reason, None
        self._by_req.get(app.req_id, set()).discard(app.application_id)
        if app.employee_id is not None:
            self._by_employee.get(app.employee_id, set()).discard(app.application_id)

    def _record(
        self,
        app: Application,
        from_stage: str | None,
        to_stage: str,
        from_status: str | None,
        to_status: str,
        reason: str,
        ms: int,
        by: str,
    ) -> None:
        self.changes.append(
            StageChange(
                len(self.changes) + 1,
                app.application_id,
                from_stage,
                to_stage,
                from_status,
                to_status,
                reason,
                ms,
                by,
            )
        )
        app.updated_ms = ms

    # ------------------------------------------------------------------ offers and starts

    def _extend_offer(self, day: date, app: Application, ms: int) -> None:
        req = self._rq.reqs[app.req_id]
        free = req.seats_open - self._pending_offers[req.req_id] if req.status in ACTIVE else 0
        if free <= 0:
            self._close(app, "rejected", "position_filled", ms, self._decider(req, "offer"))
            return
        days_to_offer = (day - app.applied_on).days
        internal = app.employee_id is not None
        accepted = bool(
            self._rng.random() < accept_probability(self._config, days_to_offer, internal)
        )
        decide = max(1, round(_lognormal(self._rng, self._ats.offer.decision_delay_days)))
        offer = Offer(
            offer_id=f"O{self._next_offer:09d}",
            application_id=app.application_id,
            extended_ms=ms,
            decide_on=day + timedelta(days=decide),
            accepted=accepted,
            days_to_offer=days_to_offer,
            updated_ms=ms,
        )
        self._next_offer += 1
        self.offers[offer.offer_id] = offer
        self._pending_offers[req.req_id] += 1
        self._offer_days.setdefault(offer.decide_on.toordinal(), []).append(offer.offer_id)
        self.truth.offers[(f"{day:%Y-%m}", app.channel, internal)] += 1

    def _decide_offer(self, day: date, offer: Offer) -> None:
        app = self.applications[offer.application_id]
        req = self._rq.reqs[app.req_id]
        self._pending_offers[req.req_id] -= 1
        ms = self._at(day, app)
        offer.decided_ms = offer.updated_ms = ms
        key = (f"{day:%Y-%m}", app.channel, app.employee_id is not None)
        if app.status != "active":  # the candidate left the company meanwhile
            offer.status = "rescinded"
            return
        if not offer.accepted:
            offer.status = "declined"
            self._close(app, "offer_declined", "offer_declined", ms, "candidate")
            self.truth.declined[key] += 1
            self.truth.ht4[(ht4_bucket(offer.days_to_offer), False)] += 1
            return
        if req.status not in ACTIVE or req.seats_open <= 0:  # the last seat went elsewhere
            offer.status = "rescinded"
            self._close(app, "rejected", "position_filled", ms, self._decider(req, "offer"))
            return
        self._rq.record_accept(req.req_id, day)
        if req.status == "filled":
            self._req_closed(day, req.req_id, "position_filled")
        internal = app.employee_id is not None
        start = (
            self._ats.offer.start_delay_days.internal
            if internal
            else self._ats.offer.start_delay_days.external
        )
        offer.status = "accepted"
        offer.start_date = day + timedelta(days=max(1, round(_lognormal(self._rng, start))))
        offer.no_start = bool(self._rng.random() < self._ats.offer.no_start_probability)
        self._close(app, "hired", "offer_accepted", ms, "candidate")
        self._starts.setdefault(offer.start_date.toordinal(), []).append(offer.offer_id)
        self.truth.accepted[key] += 1
        self.truth.ht4[(ht4_bucket(offer.days_to_offer), True)] += 1

    def _start(self, day: date, offer: Offer) -> None:
        app = self.applications[offer.application_id]
        if offer.no_start:
            lo, hi = self._ats.offer.no_start_correction_delay_days
            later = day + timedelta(days=int(self._rng.integers(lo, hi + 1)))
            self._flips.setdefault(later.toordinal(), []).append(offer.offer_id)
            return
        req = self._rq.reqs[app.req_id]
        if app.employee_id is not None:
            emp = self._wf.employee(app.employee_id)
            before = (emp.team, emp.role_family, emp.job_level, emp.location_city, emp.manager_id)
            moved = self._wf.transfer(
                day,
                emp.employee_id,
                team=req.team,
                role_family=req.role_family,
                job_level=req.job_level,
                manager_id=req.hiring_manager_id,
            )
            if not moved:
                self._flip_to_no_start(day, offer, "could_not_start")
                return
            self._rq.vacancy(
                day,
                team=before[0],
                role_family=before[1],
                job_level=before[2],
                location_city=before[3],
                manager_hint=before[4],
                leaver_id=emp.employee_id,
            )
        else:
            if self._wf.world.teams and self._team_empty(req.team):
                self._flip_to_no_start(day, offer, "could_not_start")
                return
            manager = (
                self._wf.least_loaded_manager(req.team)
                if req.is_evergreen
                else req.hiring_manager_id
            )
            cand = self._candidates.candidates[app.candidate_id]
            self._wf.hire(
                day,
                first_name=cand.first_name,
                last_name=cand.last_name,
                team=req.team,
                role_family=req.role_family,
                job_level=req.job_level,
                location_city=req.location_city,
                manager_id=manager,
                ats_candidate_id=cand.candidate_id,
            )
        self.truth.hires[(f"{day:%Y-%m}", app.channel, app.employee_id is not None)] += 1

    def _flip_to_no_start(self, day: date, offer: Offer, reason: str) -> None:
        app = self.applications[offer.application_id]
        ms = self._at(day, app)
        self._record(
            app,
            app.stage,
            app.stage,
            app.status,
            "no_start",
            reason,
            ms,
            self._decider(self._rq.reqs[app.req_id], "offer"),
        )
        app.status, app.status_reason = "no_start", reason
        offer.updated_ms = ms
        self._rq.reopen_seat(app.req_id, day)
        self.truth.no_starts += 1

    # ------------------------------------------------------------------ reacting to other engines

    def _left_company(self, day: date, employee_id: str) -> None:
        for app_id in sorted(self._by_employee.get(employee_id, set())):
            app = self.applications[app_id]
            self._close(app, "withdrawn", "left_company", self._at(day, app), "system")

    def _req_closed(self, day: date, req_id: str, reason: str) -> None:
        """Reject active applications that haven't reached onsite, a few days later (§6.4)."""
        lo, hi = self._ats.req_closed_rejection_delay_days
        for app_id in sorted(self._by_req.get(req_id, set())):
            if self.applications[app_id].stage in BEFORE_ONSITE:
                when = day + timedelta(days=int(self._rng.integers(lo, hi + 1)))
                self._closures.setdefault(when.toordinal(), []).append((app_id, reason))

    # ------------------------------------------------------------------ helpers

    def _decider(self, req: Requisition, stage: str) -> str:
        if stage in ("phone_screen", "onsite"):
            return req.hiring_manager_id
        return req.recruiter_id or "system"

    def _team_empty(self, team: str) -> bool:
        return next(t for t in self._wf.world.teams if t.name == team).manager_id is None

    def _at(self, day: date, app: Application) -> int:
        return self._at_city(day, self._rq.reqs[app.req_id].location_city)

    def _at_city(self, day: date, city: str) -> int:
        """A moment in business hours on `day`, local to `city`, as epoch milliseconds."""
        lo, hi = self._hours
        return self._midnight(day, city) + int(self._rng.integers(lo, hi))

    def _midnight(self, day: date, city: str) -> int:
        key = (day, city)
        if key not in self._midnights:
            local = datetime(day.year, day.month, day.day, tzinfo=self._zone[city])
            self._midnights[key] = int(local.timestamp() * 1000)
        return self._midnights[key]


def _lognormal(rng: np.random.Generator, dist: Lognormal) -> float:
    return float(rng.lognormal(math.log(dist.median), dist.sigma))
