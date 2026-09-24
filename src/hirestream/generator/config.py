"""Typed, validated generator configuration (SPEC §6; parameters in config/generator/base.yaml).

The models reject unknown keys and out-of-range values but never change a value: tuning
parameters is an ADR-level decision, not a loader side effect.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date
from pathlib import Path
from typing import Annotated, Any, Generic, Literal, TypeVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    TypeAdapter,
    field_validator,
    model_validator,
)

SHARE_TOLERANCE = 1e-6
CHANNELS = frozenset({"career_site", "referral", "sourced", "agency", "internal"})
DIRECT_CHANNELS = frozenset({"referral", "sourced", "agency"})
GATES = ("applied", "recruiter_screen", "phone_screen", "onsite")

N = TypeVar("N", int, float)
K = TypeVar("K")
T = TypeVar("T")


def _ordered_range(value: tuple[N, N]) -> tuple[N, N]:
    if value[0] > value[1]:
        raise ValueError(f"range lower bound {value[0]} exceeds upper bound {value[1]}")
    return value


def _sums_to_one(value: dict[K, float]) -> dict[K, float]:
    total = math.fsum(value.values())
    if abs(total - 1.0) > SHARE_TOLERANCE:
        raise ValueError(f"shares must sum to 1, got {total}")
    return value


Probability = Annotated[float, Field(ge=0.0, le=1.0)]
Fraction = Annotated[float, Field(ge=0.0, le=1.0)]
IntRange = Annotated[tuple[NonNegativeInt, NonNegativeInt], AfterValidator(_ordered_range)]
FloatRange = Annotated[tuple[float, float], AfterValidator(_ordered_range)]
ProbabilityRange = Annotated[tuple[Probability, Probability], AfterValidator(_ordered_range)]
Shares = Annotated[dict[str, Probability], AfterValidator(_sums_to_one)]
IntShares = Annotated[dict[int, Probability], AfterValidator(_sums_to_one)]


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Lognormal(Strict):
    median: PositiveFloat
    sigma: NonNegativeFloat


class Meta(Strict):
    company_name: str
    seed: NonNegativeInt
    company_founded: date  # earliest possible hire date; caps initial tenure (ADR-0004)
    internal_email_domain: str
    external_email_domains: list[str] = Field(min_length=1)
    faker_locale_by_country: dict[str, str]


class Window(Strict):
    sim_start: date
    sim_end: date  # inclusive

    @model_validator(mode="after")
    def _ordered(self) -> Window:
        if self.sim_end < self.sim_start:
            raise ValueError("sim_end is before sim_start")
        return self

    @property
    def n_days(self) -> int:
        return (self.sim_end - self.sim_start).days + 1


class Scale(Strict):
    initial_headcount: PositiveInt
    org_count: PositiveInt
    teams_per_org: IntRange
    target_min_total_events: NonNegativeInt


class Preset(Strict):
    """A preset overrides `window` and `scale` only (extra keys are rejected)."""

    window: Window
    scale: Scale


class Location(Strict):
    city: str
    country: str
    tz: str
    weight: PositiveFloat

    @field_validator("tz")
    @classmethod
    def _known_tz(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"unknown IANA timezone {value!r}") from exc
        return value


class OrgModel(Strict):
    orgs: list[str] = Field(min_length=1)
    locations: list[Location] = Field(min_length=1)
    role_families: Shares
    levels: Shares
    span_of_control: IntRange

    @model_validator(mode="after")
    def _check(self) -> OrgModel:
        if len(set(self.orgs)) != len(self.orgs):
            raise ValueError("org names must be unique")
        _sums_to_one({loc.city: loc.weight for loc in self.locations})
        lo, hi = self.span_of_control
        # With hi >= 2*lo - 1, every team size can be arranged so each manager has lo..hi
        # direct reports (ADR-0004); a narrower range leaves gaps (e.g. 6..8 cannot fit 9).
        if lo < 1 or hi < 2 * lo - 1:
            raise ValueError(
                f"span_of_control {self.span_of_control} needs lo >= 1, hi >= 2*lo - 1"
            )
        return self


class Reorg(Strict):
    at: Fraction
    teams_moved: PositiveInt


class MobilityPropensity(Strict):
    tenure_in_role_threshold_days: PositiveInt
    browse_multiplier: PositiveFloat
    apply_multiplier: PositiveFloat


class Workforce(Strict):
    attrition_annual: Probability
    attrition_first_year_multiplier: PositiveFloat
    promotion_annual: Probability
    promotion_min_days_in_level: NonNegativeInt
    lateral_move_annual: Probability
    manager_change_annual: Probability
    location_change_annual: Probability
    leave_annual: Probability
    leave_duration_days: IntRange
    growth_annual: NonNegativeFloat
    backfill_probability: Probability
    backfill_open_delay_days: IntRange
    terminated_retention_days: NonNegativeInt
    reorg: Reorg
    mobility_propensity: MobilityPropensity


class Evergreen(Strict):
    per_1000_headcount: PositiveFloat
    headcount_range: IntRange
    role_families: list[str] = Field(min_length=1)
    levels: list[str] = Field(min_length=1)


class Popularity(Strict):
    pareto_alpha: Annotated[float, Field(gt=1.0)]  # the mean only exists for alpha > 1
    truncate_at: Annotated[float, Field(gt=1.0)]  # cap on the raw draw, in units of x_m (ADR-0006)
    evergreen_multiplier: PositiveFloat


class Requisitions(Strict):
    headcount_distribution: IntShares
    internal_only_share: Probability
    on_hold_probability: Probability
    on_hold_days: IntRange
    cancel_probability: Probability
    max_open_days: PositiveInt
    initial_pipeline_days: PositiveInt  # ADR-0006
    evergreen: Evergreen
    popularity: Popularity


class JobboardExternal(Strict):
    base_daily_views_per_open_req: PositiveFloat
    returning_visitor_share: Probability
    views_per_session_geometric_p: Annotated[float, Field(gt=0.0, le=1.0)]
    max_views_per_session: PositiveInt
    p_search_before_view: Probability
    p_save_given_view: Probability
    p_apply_start_given_view: Probability
    p_apply_submit_given_start: Probability
    diurnal_peaks_local_hour: list[Annotated[int, Field(ge=0, le=23)]] = Field(min_length=1)


class JobboardInternal(Strict):
    p_employee_browses_per_day: Probability
    p_apply_start_given_view: Probability
    p_apply_submit_given_start: Probability


class Bots(Strict):
    session_share: Probability
    job_views_per_session: IntRange
    distinct_visitor_pool: PositiveInt
    known_bot_user_agent_share: Probability
    seconds_between_views: Annotated[
        tuple[NonNegativeFloat, NonNegativeFloat], AfterValidator(_ordered_range)
    ]  # ADR-0007


Referrer = Literal["direct", "search_engine", "social", "email"]


class Session(Strict):
    """Session details the spec leaves open (ADR-0007)."""

    referrer_mix: Annotated[dict[Referrer, Probability], AfterValidator(_sums_to_one)]
    seconds_between_events: Lognormal
    diurnal_spread_hours: PositiveFloat
    results_position_geometric_p: Annotated[float, Field(gt=0.0, le=1.0)]
    filter_location_share: Probability
    filter_role_family_share: Probability


DayOfWeek = Literal["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


class Jobboard(Strict):
    external: JobboardExternal
    internal: JobboardInternal
    bots: Bots
    session: Session
    seasonality_by_month: dict[Annotated[int, Field(ge=1, le=12)], PositiveFloat]
    day_of_week: dict[DayOfWeek, PositiveFloat]
    posting_age_half_life_days: PositiveFloat
    device_mix_v2: Shares

    @model_validator(mode="after")
    def _complete(self) -> Jobboard:
        if set(self.seasonality_by_month) != set(range(1, 13)):
            raise ValueError("seasonality_by_month must cover months 1-12")
        if len(self.day_of_week) != 7:
            raise ValueError("day_of_week must cover all seven days")
        return self


class GateProbabilities(Strict):
    applied: Probability
    recruiter_screen: Probability
    phone_screen: Probability
    onsite: Probability


class GateModel(Strict):
    p_advance: GateProbabilities
    p_withdraw: GateProbabilities

    @model_validator(mode="after")
    def _leaves_room_for_reject(self) -> GateModel:
        for gate in GATES:
            total = getattr(self.p_advance, gate) + getattr(self.p_withdraw, gate)
            if total > 1.0 + SHARE_TOLERANCE:
                raise ValueError(f"p_advance + p_withdraw at {gate} is {total}, above 1")
        return self


class InternalExternal(Strict, Generic[T]):
    external: T
    internal: T


class StageDelays(Strict):
    applied: Lognormal
    recruiter_screen: Lognormal


class AcceptDecay(Strict):
    threshold_days: NonNegativeInt
    per_day: NonNegativeFloat


class Offer(Strict):
    decision_delay_days: Lognormal
    base_accept_probability: InternalExternal[Probability]
    accept_decay: AcceptDecay
    accept_probability_floor: Probability
    start_delay_days: InternalExternal[Lognormal]
    no_start_probability: Probability
    no_start_correction_delay_days: IntRange


class Ats(Strict):
    direct_apps_per_1000_external_views: dict[str, NonNegativeFloat]
    reapply_probability: Probability
    external: GateModel
    internal: GateModel
    first_gate_channel_multiplier: dict[str, PositiveFloat]
    stage_delay_days: StageDelays
    rejection_delay_multiplier: PositiveFloat
    interview_stage_decision_delay_days: Lognormal
    feedback_wait_cap_days: PositiveInt
    req_closed_rejection_delay_days: IntRange
    offer: Offer

    @model_validator(mode="after")
    def _channels(self) -> Ats:
        if set(self.direct_apps_per_1000_external_views) != DIRECT_CHANNELS:
            raise ValueError(f"direct_apps_per_1000_external_views keys must be {DIRECT_CHANNELS}")
        if set(self.first_gate_channel_multiplier) != CHANNELS:
            raise ValueError(f"first_gate_channel_multiplier keys must be {CHANNELS}")
        return self


class PhoneScreen(Strict):
    interviewers: PositiveInt
    duration_minutes: PositiveInt
    lead_days: Lognormal


class Onsite(Strict):
    sessions: IntRange
    duration_minutes: PositiveInt
    lead_days: Lognormal
    same_day_probability: Probability
    panel_session_probability_v2: Probability


class InterviewerSelection(Strict):
    same_org_probability: Probability
    min_level_offset: int
    popularity_pareto_alpha: PositiveFloat
    weekly_soft_cap: PositiveInt
    over_cap_weight_multiplier: PositiveFloat


class Reschedule(Strict):
    probability: Probability
    max_times: NonNegativeInt
    delay_days: Lognormal
    initiated_by: Shares


class NoShow(Strict):
    candidate: Probability
    interviewer: Probability


class RecommendationGivenDecision(Strict):
    advance: Shares
    reject: Shares


class Feedback(Strict):
    latency_hours: Lognormal
    overload_multiplier: PositiveFloat
    chronic_slow_share: Probability
    chronic_slow_multiplier: PositiveFloat
    never_submitted_probability: Probability
    update_probability: Probability
    recommendation_given_decision: RecommendationGivenDecision
    word_count: Lognormal


class Scheduling(Strict):
    phone_screen: PhoneScreen
    onsite: Onsite
    business_hours_local: IntRange
    weekdays_only: bool
    interviewer_selection: InterviewerSelection
    reschedule: Reschedule
    cancel_probability: Probability
    no_show: NoShow
    no_show_reschedule_probability: Probability
    feedback: Feedback


class DelayComponent(Strict):
    share: Probability
    seconds: IntRange


class StreamChaos(Strict):
    duplicate_rate: Probability
    duplicate_extra_lag_seconds: IntRange
    delivery_lag: list[DelayComponent] = Field(min_length=1)
    malformed_rate: Probability
    malformed_kinds: list[Literal["truncated_json", "missing_required_field", "invalid_enum"]]

    @model_validator(mode="after")
    def _mixture(self) -> StreamChaos:
        _sums_to_one({i: c.share for i, c in enumerate(self.delivery_lag)})
        return self


class SchedulingTzBug(Strict):
    producer_version: str
    at: Fraction
    duration_days: PositiveInt
    missing_timezone_share: Probability


class SchemaV2(Strict):
    scheduling_at: Fraction
    jobboard_at: Fraction


class FieldRename(Strict):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    from_: str = Field(alias="from")
    to: str


class ColumnRename(FieldRename):
    at: Fraction
    days: PositiveInt


class HrisChaos(Strict):
    missing_snapshot_at: list[Fraction]
    column_rename: ColumnRename
    retro_effective_share: Probability
    retro_effective_days: IntRange
    duplicate_row_at: Fraction


class Chaos(Strict):
    streams: StreamChaos
    scheduling_tz_bug: SchedulingTzBug
    schema_v2: SchemaV2
    hris: HrisChaos


StreamSource = Literal["scheduling", "jobboard"]


class DuplicateStorm(Strict):
    at: Fraction
    hours: PositiveInt
    duplicate_rate: Probability
    source: StreamSource


class SilentSchemaBreak(Strict):
    at: Fraction
    days: PositiveInt
    source: StreamSource
    rename_field: FieldRename
    schema_version_unchanged: bool


class LateBurst(Strict):
    at: Fraction
    hours: PositiveInt
    delay_days: PositiveInt
    source: StreamSource


class HrisPartialFile(Strict):
    at: Fraction
    keep_fraction: Probability


class Incidents(Strict):
    """All off by default; enabled per run with `--incident NAME` (SPEC §6.8)."""

    duplicate_storm: DuplicateStorm
    silent_schema_break: SilentSchemaBreak
    late_burst: LateBurst
    hris_partial_file: HrisPartialFile


INCIDENT_NAMES: tuple[str, ...] = tuple(Incidents.model_fields)


class CalibrationTargets(Strict):
    req_fill_rate: ProbabilityRange
    median_time_to_fill_days: FloatRange
    median_time_to_hire_days: FloatRange
    applications_per_hire: FloatRange
    offer_acceptance_rate: ProbabilityRange
    internal_fill_rate: ProbabilityRange
    application_channel_mix: dict[str, ProbabilityRange]
    clickstream_bot_event_share: ProbabilityRange
    feedback_within_48h_share: ProbabilityRange

    @field_validator("application_channel_mix")
    @classmethod
    def _all_channels(cls, value: dict[str, tuple[float, float]]) -> dict[str, tuple[float, float]]:
        if set(value) != CHANNELS:
            raise ValueError(f"application_channel_mix keys must be {CHANNELS}")
        return value


class Kinesis(Strict):
    put_records_batch_max: Annotated[int, Field(ge=1, le=500)]  # AWS PutRecords limit
    max_retries: NonNegativeInt
    partition_key: dict[StreamSource, str]


class Output(Strict):
    stream_file_max_events: PositiveInt
    stream_file_compression: Literal["gzip"]
    hris_compression: Literal["gzip"]
    ats_copy_batch_rows: PositiveInt
    kinesis: Kinesis


class LiveTail(Strict):
    sim_days_per_wall_minute: PositiveFloat


class GeneratorConfig(Strict):
    """The resolved configuration for one run: base.yaml with one preset applied."""

    preset: str
    meta: Meta
    window: Window
    scale: Scale
    org_model: OrgModel
    workforce: Workforce
    requisitions: Requisitions
    jobboard: Jobboard
    ats: Ats
    scheduling: Scheduling
    chaos: Chaos
    incidents: Incidents
    hidden_truths: dict[str, str]
    calibration_targets: CalibrationTargets
    output: Output
    live_tail: LiveTail

    @model_validator(mode="after")
    def _cross_section(self) -> GeneratorConfig:
        if self.meta.company_founded >= self.window.sim_start:
            raise ValueError(
                f"meta.company_founded {self.meta.company_founded} must be before "
                f"sim_start {self.window.sim_start}"
            )
        if self.scale.org_count > len(self.org_model.orgs):
            raise ValueError(
                f"scale.org_count {self.scale.org_count} exceeds the "
                f"{len(self.org_model.orgs)} orgs in org_model.orgs"
            )
        countries = {loc.country for loc in self.org_model.locations}
        if missing := countries - set(self.meta.faker_locale_by_country):
            raise ValueError(f"no Faker locale for countries {sorted(missing)}")
        evergreen = self.requisitions.evergreen
        if unknown := set(evergreen.role_families) - set(self.org_model.role_families):
            raise ValueError(f"evergreen role_families not in org_model: {sorted(unknown)}")
        if unknown := set(evergreen.levels) - set(self.org_model.levels):
            raise ValueError(f"evergreen levels not in org_model: {sorted(unknown)}")
        ht3, internal = self.workforce.mobility_propensity, self.jobboard.internal
        for name, p, multiplier in (
            (
                "p_employee_browses_per_day",
                internal.p_employee_browses_per_day,
                ht3.browse_multiplier,
            ),
            ("p_apply_start_given_view", internal.p_apply_start_given_view, ht3.apply_multiplier),
        ):
            if p * multiplier > 1:
                raise ValueError(f"jobboard.internal.{name} x HT3 multiplier exceeds 1")
        return self


_PRESETS = TypeAdapter(dict[str, Preset])


def load_config(path: Path, preset: str | None) -> GeneratorConfig:
    """Load base.yaml and apply `preset` (None keeps the base window and scale)."""
    raw: dict[str, Any] = yaml.safe_load(path.read_text())
    presets = _PRESETS.validate_python(raw.pop("presets", {}))
    if preset is None:
        return GeneratorConfig.model_validate({**raw, "preset": "base"})
    if preset not in presets:
        raise KeyError(f"unknown preset {preset!r}; choose from {sorted(presets)}")
    chosen = presets[preset]
    return GeneratorConfig.model_validate(
        {**raw, "preset": preset, "window": chosen.window, "scale": chosen.scale}
    )


def preset_names(path: Path) -> list[str]:
    raw: dict[str, Any] = yaml.safe_load(path.read_text())
    return sorted(raw.get("presets", {}))


def config_hash(config: GeneratorConfig) -> str:
    """sha256 of the resolved config as canonical JSON; recorded in every run manifest."""
    canonical = json.dumps(
        config.model_dump(mode="json", by_alias=True), sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode()).hexdigest()
