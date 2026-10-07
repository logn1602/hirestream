"""Scheduling service: interviews for the ATS's phone screens and onsite loops.

SPEC §6.7 sets the rules and §7.2 the events; ADR-0010 fixes what the spec leaves open. When an
application enters an interview stage, the ATS calls `begin`. Interviews are booked with
interviewers chosen by (capped) popularity and weekly load, and then live day by day: they may be
rescheduled, cancelled and replaced, missed, or completed, after which feedback arrives with HT1's
overload slowdown. Once every interview in the stage has feedback, or the waiting cap has passed,
the scheduler tells the ATS the stage is ready for its decision. Time in these stages is therefore
emergent: slow feedback slows hiring.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal, Protocol
from zoneinfo import ZoneInfo

import numpy as np
import numpy.typing as npt

from hirestream.generator.config import GeneratorConfig, Lognormal
from hirestream.generator.events import EventSink, StreamEvent, iso_utc_ms, uuid4_str
from hirestream.generator.requisitions import Requisition, Requisitions
from hirestream.generator.sampling import sample_truncated_pareto
from hirestream.generator.workforce import Workforce

SOURCE = "scheduling-service"
PRODUCER_VERSION = "1.2.4"  # the timezone-bug build (1.3.0) and schema v2 arrive in T1.7b
HOUR_MS = 3_600_000
DAY_MS = 86_400_000
SLOT_MS = 15 * 60_000  # interviews start on the quarter hour

InterviewType = Literal["phone_screen", "onsite"]
Status = Literal["scheduled", "completed", "cancelled", "no_show_candidate", "no_show_interviewer"]
CANCEL_REASONS = frozenset(
    {"candidate_withdrew", "position_filled", "interviewer_unavailable", "other"}
)
RESCHEDULE_REASON = {
    "candidate": "candidate_conflict",
    "interviewer": "interviewer_conflict",
    "coordinator": "other",
}


class Applicant(Protocol):
    """What the scheduler needs to know about an application (read-only)."""

    @property
    def application_id(self) -> str: ...
    @property
    def req_id(self) -> str: ...
    @property
    def employee_id(self) -> str | None: ...


@dataclass(slots=True)
class Interview:
    interview_id: str
    application_id: str
    req_id: str
    interview_type: InterviewType
    loop_id: str | None
    session_index: int | None
    interviewer_id: str
    coordinator_id: str
    tz: str
    duration_minutes: int
    start_ms: int
    original_start_ms: int
    status: Status = "scheduled"
    reschedules: int = 0
    last_event_ms: int = 0  # keeps each interview's events in order across timezones


@dataclass(slots=True)
class _Stage:
    application_id: str
    stage: InterviewType
    advance: bool  # the decision the ATS drew; recommendations follow it (§6.6)
    employee_id: str | None = None  # an internal applicant can't interview themselves
    open: set[str] = field(default_factory=set)  # interviews still to happen
    awaiting: set[str] = field(default_factory=set)  # completed, feedback not in yet
    last_done_ms: int = 0
    closed: bool = False
    reported: bool = False


@dataclass(slots=True)
class _Roster:
    """Who could interview today, by world index; built at the day's first booking."""

    day: date
    active: npt.NDArray[np.bool_]
    rank: npt.NDArray[np.int64]
    org: npt.NDArray[np.str_]
    pools: dict[
        tuple[str | None, int, bool], tuple[npt.NDArray[np.intp], npt.NDArray[np.float64]]
    ] = field(default_factory=dict)


@dataclass
class SchedulingTruth:
    """What actually happened, before any chaos (SPEC §6.10): HT1, SLAs, and disruptions."""

    interviews: Counter[str] = field(default_factory=Counter)  # by type, replacements included
    completed: int = 0
    cancelled: Counter[str] = field(default_factory=Counter)  # by reason
    no_shows: Counter[str] = field(default_factory=Counter)  # by party
    reschedules: int = 0
    feedback: int = 0
    never_submitted: int = 0
    updates: int = 0
    latency_hours: dict[str, float] = field(default_factory=dict)  # submitted feedback only


class Scheduler:
    def __init__(
        self,
        config: GeneratorConfig,
        rng: np.random.Generator,
        workforce: Workforce,
        requisitions: Requisitions,
    ) -> None:
        self.truth = SchedulingTruth()
        self.interviews: dict[str, Interview] = {}
        self._config = config
        self._cfg = config.scheduling
        self._rng = rng
        self._wf = workforce
        self._rq = requisitions
        self._tz = {loc.city: loc.tz for loc in config.org_model.locations}
        self._zones = {loc.tz: ZoneInfo(loc.tz) for loc in config.org_model.locations}
        self._levels = list(config.org_model.levels)
        lo, hi = self._cfg.business_hours_local
        self._hours = (lo * HOUR_MS, hi * HOUR_MS)
        self._agenda: dict[int, list[tuple[str, str]]] = {}
        self._stages: dict[str, _Stage] = {}  # application -> its current interview stage
        self._feedback: dict[str, tuple[int, float, str, int, str]] = {}  # (ms, hours, rec, …)
        self._updates: dict[str, tuple[int, str]] = {}  # interview -> (ms, revised recommendation)
        self._load: Counter[tuple[str, int, int]] = Counter()  # (interviewer, ISO year, week)
        self._popularity = np.zeros(0)
        self._slow = np.zeros(0, dtype=bool)
        self._trained = np.zeros(0, dtype=bool)
        self._roster: _Roster | None = None
        self._outbox: list[StreamEvent] = []
        self._next = {"interview": 1, "loop": 1, "feedback": 1}
        self._loops: dict[str, set[str]] = {}  # loop -> everyone booked on it, replacements too
        self._on_ready: Callable[[str, date], None] | None = None

    # ------------------------------------------------------------------ the ATS's interface

    def connect(self, on_ready: Callable[[str, date], None]) -> None:
        self._on_ready = on_ready

    def begin(self, day: date, app: Applicant, stage: str, outcome: str) -> date:
        """Book the stage's interviews; returns the date of the first one."""
        kind: InterviewType = "phone_screen" if stage == "phone_screen" else "onsite"
        req = self._rq.reqs[app.req_id]
        st = _Stage(app.application_id, kind, outcome == "advance", app.employee_id)
        self._stages[app.application_id] = st
        if kind == "phone_screen":
            target = self._business_day(day + timedelta(days=self._lead("phone_screen")))
            booked = [self._book(day, req, app, st, None, None, target, set())]
        else:
            booked = self._book_loop(day, req, app, st)
        return min(self._utc_date(iv.start_ms) for iv in booked)

    def cancel(self, at_ms: int, application_id: str, reason: str) -> None:
        """The application closed at `at_ms` (withdrawal, closure): cancel what hasn't happened."""
        st = self._stages.get(application_id)
        if st is None or st.closed:
            return
        st.closed = True
        for interview_id in sorted(st.open):
            lag = int(self._rng.integers(5, 61)) * 60_000  # the coordinator reacts within the hour
            self._cancel(self.interviews[interview_id], reason, at_ms + lag)

    def step(self, day: date, sink: EventSink) -> None:
        """Run today's reschedules, cancellations, interviews, and feedback; flush events."""
        # Actions can add more for today (feedback minutes after an interview), so drain.
        while actions := self._agenda.pop(day.toordinal(), None):
            for action, key in actions:
                self._run(day, action, key)
        events = sorted(self._outbox, key=lambda e: e.event_ts_ms)
        self._outbox = []
        sink.write(events)

    def summary(self) -> dict[str, float]:
        """Headline truth. HT1 labels feedback by the interviewer's final load that ISO week,
        the way the warehouse will (SPEC §9 `is_overloaded`), not by the load when it was drawn.
        """
        cap = self._cfg.interviewer_selection.weekly_soft_cap
        latency: dict[bool, list[float]] = {False: [], True: []}
        for interview_id, hours in self.truth.latency_hours.items():
            iv = self.interviews[interview_id]
            latency[self._load[(iv.interviewer_id, *self._week(iv.start_ms))] > cap].append(hours)
        normal, overloaded = latency[False], latency[True]
        on_time = sum(h <= 48 for h in self.truth.latency_hours.values())
        return {
            "interviews": sum(self.truth.interviews.values()),
            "completed": self.truth.completed,
            "cancelled": sum(self.truth.cancelled.values()),
            "no_shows": sum(self.truth.no_shows.values()),
            "reschedules": self.truth.reschedules,
            "feedback": self.truth.feedback,
            "overloaded_feedback": len(overloaded),
            "within_48h": on_time / self.truth.completed if self.truth.completed else 0.0,
            "ht1_ratio": (
                float(np.median(overloaded) / np.median(normal)) if normal and overloaded else 0.0
            ),
        }

    def _run(self, day: date, action: str, key: str) -> None:
        if action == "ready":
            self._check_ready(day, key)
            return
        iv = self.interviews[key]
        if action == "feedback":
            self._submit_feedback(day, iv)
        elif action == "update":
            self._update_feedback(iv)
        elif iv.status != "scheduled":
            return  # cancelled in the meantime
        elif action == "reschedule":
            self._reschedule(day, iv, self._draw_initiator(), self._business_moment(day, iv.tz))
        elif action == "cancel":
            self._cancel(iv, "other", self._business_moment(day, iv.tz))
            self._replace(day, iv)
        elif action == "happen":
            self._happen(day, iv)

    # ------------------------------------------------------------------ booking

    def _book_loop(
        self, day: date, req: Requisition, app: Applicant, st: _Stage
    ) -> list[Interview]:
        onsite = self._cfg.onsite
        lo, hi = onsite.sessions
        sessions = int(self._rng.integers(lo, hi + 1))
        loop_id = self._new_id("loop", "L", 8)
        first = self._business_day(day + timedelta(days=self._lead("onsite")))
        tz = self._tz[req.location_city]  # the loop happens at the req's office (ADR-0010)
        booked: list[Interview] = []
        used = self._loops.setdefault(loop_id, set())  # a loop never repeats an interviewer
        if self._rng.random() < onsite.same_day_probability:
            span = sessions * onsite.duration_minutes * 60_000
            base = self._slot_ms(first, tz, span)
            for i in range(sessions):
                start = base + i * onsite.duration_minutes * 60_000
                booked.append(self._book(day, req, app, st, loop_id, i + 1, first, used, start, tz))
        else:  # one session per business day
            target = first
            for i in range(sessions):
                booked.append(self._book(day, req, app, st, loop_id, i + 1, target, used, tz=tz))
                target = self._business_day(target + timedelta(days=1))
        return booked

    def _book(
        self,
        day: date,
        req: Requisition,
        app: Applicant,
        st: _Stage,
        loop_id: str | None,
        session: int | None,
        target: date,
        used: set[str],
        start_ms: int | None = None,
        tz: str | None = None,
        not_before_ms: int = 0,
    ) -> Interview:
        kind = st.stage
        interviewer = self._select(day, req, app, target, used)
        used.add(interviewer)
        tz = tz or self._tz[self._wf.employee(interviewer).location_city]
        duration = (
            self._cfg.phone_screen if kind == "phone_screen" else self._cfg.onsite
        ).duration_minutes
        start = start_ms if start_ms is not None else self._slot_ms(target, tz, duration * 60_000)
        iv = Interview(
            interview_id=self._new_id("interview", "I", 9),
            application_id=app.application_id,
            req_id=req.req_id,
            interview_type=kind,
            loop_id=loop_id,
            session_index=session,
            interviewer_id=interviewer,
            coordinator_id=req.recruiter_id or req.hiring_manager_id,
            tz=tz,
            duration_minutes=duration,
            start_ms=start,
            original_start_ms=start,
        )
        self.interviews[iv.interview_id] = iv
        st.open.add(iv.interview_id)
        self._count(iv, +1)
        self.truth.interviews[kind] += 1
        booked_at = min(self._business_moment(day, self._tz[req.location_city]), start - HOUR_MS)
        booked_at = max(booked_at, not_before_ms)
        self._emit(
            "interview_scheduled",
            booked_at,
            iv,
            {
                "interview_id": iv.interview_id,
                "application_id": iv.application_id,
                "req_id": iv.req_id,
                "interview_type": kind,
                "loop_id": loop_id,
                "session_index": session,
                "interviewer_id": interviewer,
                "scheduled_start": self._local_iso(start, tz),
                "duration_minutes": duration,
                "timezone": tz,
                "coordinator_id": iv.coordinator_id,
            },
        )
        self._plan(day, iv)
        return iv

    def _plan(self, day: date, iv: Interview) -> None:
        """Decide what happens next to a scheduled interview: move it, cancel it, or hold it."""
        start_day = self._utc_date(iv.start_ms)
        lead = (start_day - day).days
        u = self._rng.random()
        p_move, p_cancel = self._cfg.reschedule.probability, self._cfg.cancel_probability
        if u < p_move:  # past max_times, a drawn reschedule just doesn't happen
            action = "reschedule" if iv.reschedules < self._cfg.reschedule.max_times else "happen"
        else:
            action = "cancel" if u < p_move + p_cancel else "happen"
        if action == "happen" or lead < 1:
            self._at(start_day, "happen", iv.interview_id)
        else:  # some day before the interview
            self._at(
                day + timedelta(days=int(self._rng.integers(0, lead))), action, iv.interview_id
            )

    def _select(
        self, day: date, req: Requisition, app: Applicant, target: date, used: set[str]
    ) -> str:
        """Popularity-weighted, load-aware choice of a trained interviewer (§6.7, ADR-0010).

        Same org first (with `same_org_probability`), then any org. If every trained candidate
        is taken or far past the cap, any eligible employee steps in; the hiring manager last.
        """
        sel = self._cfg.interviewer_selection
        top = len(self._levels) - 1
        rank = max(0, min(self._levels.index(req.job_level) + sel.min_level_offset, top))
        same_org = self._rng.random() < sel.same_org_probability
        excluded = used | ({app.employee_id} if app.employee_id else set())
        week = target.isocalendar()[:2]
        emps = self._wf.world.employees
        searches = [(req.org, True)] if same_org else []
        for org, trained in [*searches, (None, True), (None, False)]:
            idx, cumulative = self._pool(day, org, rank, trained)
            if not len(idx):
                continue
            for _ in range(64):  # accept/reject: popularity, then the load penalty
                u = self._rng.random() * cumulative[-1]
                k = min(int(np.searchsorted(cumulative, u, side="right")), len(idx) - 1)
                who = emps[int(idx[k])].employee_id
                if who in excluded:
                    continue
                past_cap = self._load[(who, *week)] - sel.weekly_soft_cap + 1
                if past_cap > 0 and self._rng.random() >= sel.over_cap_weight_multiplier**past_cap:
                    continue  # the weight shrinks again with every interview at or past the cap
                return who
        return req.hiring_manager_id

    def _pool(
        self, day: date, org: str | None, rank: int, trained: bool
    ) -> tuple[npt.NDArray[np.intp], npt.NDArray[np.float64]]:
        """World indices of today's candidates at or above `rank` (in `org`; trained ones only
        unless `trained` is False), with their cumulative popularity.
        """
        if self._roster is None or self._roster.day != day:
            self._grow_traits()
            emps = self._wf.world.employees  # later hires can interview from tomorrow
            self._roster = _Roster(
                day, self._wf.active_mask(), self._wf.level_ranks(), np.array([e.org for e in emps])
            )
        roster = self._roster
        key = (org, rank, trained)
        if key not in roster.pools:
            eligible = roster.active & (roster.rank >= rank)
            if trained:
                eligible &= self._trained[: len(eligible)]
            if org is not None:
                eligible &= roster.org == org
            idx = np.flatnonzero(eligible)
            roster.pools[key] = (idx, np.cumsum(self._popularity[idx]))
        return roster.pools[key]

    def _grow_traits(self) -> None:
        """Popularity, interviewer training, and chronic slowness for employees seen for the
        first time (hires too): stable traits, drawn once.

        Among trained interviewers, slowness is blocked by popularity: systematic sampling down
        the popularity ranking from a random start. Everyone keeps the same chance of being slow,
        but the busiest interviewers are slow as often as the rest. HT1 is measured on a handful
        of heavily loaded people, and independent draws would let luck decide whether they are
        slow and swing the ratio (ADR-0010).
        """
        missing = len(self._wf.world.employees) - len(self._popularity)
        if missing <= 0:
            return
        sel, fb = self._cfg.interviewer_selection, self._cfg.feedback
        pop = sample_truncated_pareto(
            self._rng, sel.popularity_pareto_alpha, sel.popularity_truncate_at, missing
        )
        trained = self._rng.random(missing) < sel.trained_share
        slow = self._rng.random(missing) < fb.chronic_slow_share  # untrained: independent draws
        ranked = np.flatnonzero(trained)[np.argsort(-pop[trained], kind="stable")]
        start = self._rng.random()
        slow[ranked] = False
        if fb.chronic_slow_share > 0:
            step = 1 / fb.chronic_slow_share
            positions = ((start + np.arange(math.ceil(len(ranked) / step) + 1)) * step).astype(int)
            slow[ranked[positions[positions < len(ranked)]]] = True
        self._popularity = np.concatenate([self._popularity, pop])
        self._slow = np.concatenate([self._slow, slow])
        self._trained = np.concatenate([self._trained, trained])

    # ------------------------------------------------------------------ what happens to interviews

    def _reschedule(self, day: date, iv: Interview, initiated_by: str, at_ms: int) -> None:
        delay = max(1, round(_lognormal(self._rng, self._cfg.reschedule.delay_days)))
        target = self._business_day(max(day, self._utc_date(iv.start_ms)) + timedelta(days=delay))
        new_start = self._slot_ms(target, iv.tz, iv.duration_minutes * 60_000)
        self._emit(
            "interview_rescheduled",
            at_ms,
            iv,
            {
                "interview_id": iv.interview_id,
                "previous_start": self._local_iso(iv.start_ms, iv.tz),
                "new_start": self._local_iso(new_start, iv.tz),
                "timezone": iv.tz,
                "reason": RESCHEDULE_REASON[initiated_by],
                "initiated_by": initiated_by,
            },
        )
        self._count(iv, -1)
        iv.start_ms, iv.status = new_start, "scheduled"
        iv.reschedules += 1
        self._count(iv, +1)
        self.truth.reschedules += 1
        self._plan(day, iv)

    def _cancel(self, iv: Interview, reason: str, at_ms: int) -> None:
        self._emit(
            "interview_cancelled",
            min(at_ms, iv.start_ms),
            iv,
            {"interview_id": iv.interview_id, "reason": reason},
        )
        iv.status = "cancelled"
        self._count(iv, -1)
        self.truth.cancelled[reason] += 1
        st = self._stages.get(iv.application_id)
        if st is not None:
            st.open.discard(iv.interview_id)

    def _replace(self, day: date, iv: Interview) -> None:
        """Book a new interview (new id, new interviewer) for a cancelled one."""
        st = self._stages.get(iv.application_id)
        if st is None or st.closed:
            return
        req = self._rq.reqs[iv.req_id]
        app = _Ref(iv.application_id, iv.req_id, st.employee_id)
        target = self._business_day(day + timedelta(days=self._lead(iv.interview_type)))
        if iv.loop_id is None:
            used, tz = {iv.interviewer_id}, None
        else:
            used, tz = self._loops[iv.loop_id], iv.tz
        after = iv.last_event_ms + int(self._rng.integers(5, 61)) * 60_000  # after the cancel
        self._book(
            day,
            req,
            app,
            st,
            iv.loop_id,
            iv.session_index,
            target,
            used,
            tz=tz,
            not_before_ms=after,
        )

    def _happen(self, day: date, iv: Interview) -> None:
        st = self._stages[iv.application_id]
        interviewer = self._wf.employee(iv.interviewer_id)
        if interviewer.employment_status != "active":  # left or on leave since booking
            self._cancel(iv, "interviewer_unavailable", iv.start_ms - 2 * HOUR_MS)
            self._replace(day, iv)
            return
        no_show = self._cfg.no_show
        u = self._rng.random()
        party = (
            "candidate"
            if u < no_show.candidate
            else "interviewer"
            if u < no_show.candidate + no_show.interviewer
            else None
        )
        if party is not None:
            recorded = iv.start_ms + self._cfg.no_show_recorded_after_minutes * 60_000
            self._emit(
                "interview_no_show",
                recorded,
                iv,
                {"interview_id": iv.interview_id, "no_show_party": party},
            )
            iv.status = "no_show_candidate" if party == "candidate" else "no_show_interviewer"
            self.truth.no_shows[party] += 1
            rebook = self._rng.random() < self._cfg.no_show_reschedule_probability
            if rebook and iv.reschedules < self._cfg.reschedule.max_times:
                self._reschedule(day, iv, "coordinator", recorded + 60_000)
            else:
                st.open.discard(iv.interview_id)  # the decision proceeds without it (§6.7)
                self._maybe_wait(day, st)
            return
        lo, hi = self._cfg.completion_jitter_minutes
        actual_start = iv.start_ms + int(self._rng.integers(lo, hi + 1)) * 60_000
        actual_end = actual_start + iv.duration_minutes * 60_000
        self._emit(
            "interview_completed",
            actual_end,
            iv,
            {
                "interview_id": iv.interview_id,
                "actual_start": iso_utc_ms(actual_start),
                "actual_end": iso_utc_ms(actual_end),
            },
        )
        iv.status = "completed"
        self.truth.completed += 1
        st.open.discard(iv.interview_id)
        st.awaiting.add(iv.interview_id)
        st.last_done_ms = max(st.last_done_ms, actual_end)
        self._plan_feedback(iv, st, actual_end)
        self._maybe_wait(day, st)

    def _plan_feedback(self, iv: Interview, st: _Stage, done_ms: int) -> None:
        fb = self._cfg.feedback
        if self._rng.random() < fb.never_submitted_probability:
            self.truth.never_submitted += 1
            return  # the stage waits for the cap instead
        overloaded = (
            self._load[(iv.interviewer_id, *self._week(iv.start_ms))]
            > self._cfg.interviewer_selection.weekly_soft_cap
        )
        slow = self._is_slow(iv.interviewer_id)
        hours = _lognormal(self._rng, fb.latency_hours)
        hours *= (fb.overload_multiplier if overloaded else 1.0) * (
            fb.chronic_slow_multiplier if slow else 1.0
        )
        at = done_ms + int(hours * HOUR_MS)
        dist = (
            fb.recommendation_given_decision.advance
            if st.advance
            else fb.recommendation_given_decision.reject
        )
        rec = self._choice(dist)
        words = max(1, round(_lognormal(self._rng, fb.word_count)))
        self._feedback[iv.interview_id] = (at, hours, rec, words, self._new_id("feedback", "F", 9))
        self._at(self._utc_date(at), "feedback", iv.interview_id)
        if self._rng.random() < fb.update_probability:
            later = at + int(_lognormal(self._rng, fb.latency_hours) * HOUR_MS)
            self._updates[iv.interview_id] = (later, self._choice(dist))

    def _submit_feedback(self, day: date, iv: Interview) -> None:
        at, hours, rec, words, feedback_id = self._feedback[iv.interview_id]
        self._emit(
            "feedback_submitted",
            at,
            iv,
            {
                "feedback_id": feedback_id,
                "interview_id": iv.interview_id,
                "interviewer_id": iv.interviewer_id,
                "recommendation": rec,
                "word_count": words,
            },
        )
        self.truth.feedback += 1
        self.truth.latency_hours[iv.interview_id] = hours
        update = self._updates.get(iv.interview_id)
        if update is not None:
            self._at(self._utc_date(update[0]), "update", iv.interview_id)
        st = self._stages.get(iv.application_id)
        if st is not None:
            st.awaiting.discard(iv.interview_id)
            self._check_ready(day, iv.application_id)

    def _update_feedback(self, iv: Interview) -> None:
        at, rec = self._updates.pop(iv.interview_id)
        feedback_id = self._feedback[iv.interview_id][-1]
        self._emit(
            "feedback_updated",
            at,
            iv,
            {
                "feedback_id": feedback_id,
                "interview_id": iv.interview_id,
                "interviewer_id": iv.interviewer_id,
                "recommendation": rec,
            },
        )
        self.truth.updates += 1

    # ------------------------------------------------------------------ stage readiness

    def _maybe_wait(self, day: date, st: _Stage) -> None:
        """Once nothing is left to happen, wait for feedback, but no longer than the cap."""
        if st.open:
            return
        if st.awaiting:
            cap = self._utc_date(st.last_done_ms) + timedelta(
                days=self._config.ats.feedback_wait_cap_days
            )
            self._at(cap, "ready", st.application_id)
        self._check_ready(day, st.application_id)

    def _check_ready(self, day: date, application_id: str) -> None:
        st = self._stages.get(application_id)
        if st is None or st.closed or st.reported or st.open:
            return
        if st.awaiting:
            cap = self._utc_date(st.last_done_ms) + timedelta(
                days=self._config.ats.feedback_wait_cap_days
            )
            if day < cap:
                return
        st.reported = True
        if self._on_ready is not None:
            self._on_ready(application_id, day)

    # ------------------------------------------------------------------ helpers

    def _emit(self, event_type: str, ts_ms: int, iv: Interview, payload: dict[str, Any]) -> None:
        ts_ms = max(ts_ms, iv.last_event_ms + 1000) if iv.last_event_ms else ts_ms
        iv.last_event_ms = ts_ms
        high, low = (
            int(x)
            for x in self._rng.integers(
                0, np.iinfo(np.uint64).max, size=2, dtype=np.uint64, endpoint=True
            )
        )
        sent = ts_ms + int(self._rng.integers(0, 5001))
        body: dict[str, Any] = {
            "event_id": uuid4_str(high, low),
            "event_type": event_type,
            "schema_version": 1,
            "source": SOURCE,
            "producer_version": PRODUCER_VERSION,
            "event_ts": iso_utc_ms(ts_ms),
            "sent_ts": iso_utc_ms(sent),
            "payload": payload,
        }
        self._outbox.append(StreamEvent(SOURCE, event_type, ts_ms, sent, iv.interview_id, body))

    def _count(self, iv: Interview, delta: int) -> None:
        self._load[(iv.interviewer_id, *self._week(iv.start_ms))] += delta

    def _week(self, ms: int) -> tuple[int, int]:
        year, week, _ = self._utc_date(ms).isocalendar()
        return year, week

    def _is_slow(self, employee_id: str) -> bool:
        self._grow_traits()
        index = self._wf.index_of(employee_id)
        return bool(self._slow[index])

    def _lead(self, kind: InterviewType) -> int:
        dist = (
            self._cfg.phone_screen.lead_days
            if kind == "phone_screen"
            else self._cfg.onsite.lead_days
        )
        return max(1, round(_lognormal(self._rng, dist)))

    def _business_day(self, day: date) -> date:
        while self._cfg.weekdays_only and day.weekday() >= 5:
            day += timedelta(days=1)
        return day

    def _slot_ms(self, day: date, tz: str, span_ms: int) -> int:
        """A quarter-hour start in business hours on `day`, local to `tz`, fitting `span_ms`."""
        lo, hi = self._hours
        slots = max(1, (hi - lo - span_ms) // SLOT_MS + 1)
        offset = lo + int(self._rng.integers(0, slots)) * SLOT_MS
        return self._midnight(day, tz) + offset

    def _business_moment(self, day: date, tz: str) -> int:
        lo, hi = self._hours
        return self._midnight(day, tz) + int(self._rng.integers(lo, hi))

    def _midnight(self, day: date, tz: str) -> int:
        local = datetime(day.year, day.month, day.day, tzinfo=self._zones[tz])
        return int(local.timestamp() * 1000)

    def _local_iso(self, ms: int, tz: str) -> str:
        """Local ISO-8601 with its UTC offset, e.g. 2025-02-03T10:00:00-08:00 (SPEC §7.2)."""
        return datetime.fromtimestamp(ms / 1000, self._zones[tz]).isoformat(timespec="seconds")

    @staticmethod
    def _utc_date(ms: int) -> date:
        return datetime.fromtimestamp(ms / 1000, UTC).date()

    def _at(self, day: date, action: str, key: str) -> None:
        self._agenda.setdefault(day.toordinal(), []).append((action, key))

    def _draw_initiator(self) -> str:
        return self._choice(self._cfg.reschedule.initiated_by)

    def _choice(self, shares: Mapping[str, float]) -> str:
        keys = list(shares)
        weights = np.asarray(list(shares.values()), dtype=float)
        return keys[int(self._rng.choice(len(keys), p=weights / weights.sum()))]

    def _new_id(self, kind: str, prefix: str, width: int) -> str:
        n = self._next[kind]
        self._next[kind] = n + 1
        return f"{prefix}{n:0{width}d}"


@dataclass(frozen=True, slots=True)
class _Ref:
    application_id: str
    req_id: str
    employee_id: str | None


def _lognormal(rng: np.random.Generator, dist: Lognormal) -> float:
    return float(rng.lognormal(math.log(dist.median), dist.sigma))
