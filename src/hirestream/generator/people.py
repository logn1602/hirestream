"""Synthetic person names and email addresses (SPEC §6.1: Faker only for person attributes).

Only first and last names are generated: no titles, no gender, no demographic attributes.
Every address uses a reserved `.example` domain from the config, so none can reach a real inbox.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping

from faker import Faker

_NOT_ALNUM = re.compile(r"[^a-z0-9]+")


class PersonNamer:
    """One seeded Faker per country, each using that country's locale from the config."""

    def __init__(self, locale_by_country: Mapping[str, str], seed: int) -> None:
        self._fakers: dict[str, Faker] = {}
        for country, locale in sorted(locale_by_country.items()):
            fake = Faker(locale)
            # Instance seeding, never Faker.seed(): the class-level seed is global state.
            # A str seed is hashed with sha512 by `random`, so it is stable across processes.
            fake.seed_instance(f"{seed}:{country}")
            self._fakers[country] = fake

    def name(self, country: str) -> tuple[str, str]:
        fake = self._fakers[country]
        return fake.first_name(), fake.last_name()


def email_local_part(first: str, last: str) -> str:
    """`first.last`, folded to lowercase ASCII letters and digits (accents and curly
    apostrophes dropped, so D\u2019Alia becomes `dalia`)."""
    return f"{_fold(first)}.{_fold(last)}"


def _fold(text: str) -> str:
    ascii_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return _NOT_ALNUM.sub("", ascii_text.lower()) or "x"


class EmailAllocator:
    """Hands out unique addresses in call order: jane.doe, jane.doe2, jane.doe3, …"""

    def __init__(self, domain: str) -> None:
        self.domain = domain
        self._used: set[str] = set()

    def allocate(self, first: str, last: str) -> str:
        base = email_local_part(first, last)
        local, n = base, 1
        while local in self._used:
            n += 1
            local = f"{base}{n}"
        self._used.add(local)
        return f"{local}@{self.domain}"
