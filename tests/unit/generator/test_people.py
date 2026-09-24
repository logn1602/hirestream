import pytest

from hirestream.generator.people import EmailAllocator, PersonNamer, email_local_part

LOCALES = {"US": "en_US", "GB": "en_GB", "IN": "en_IN"}


def _names(namer: PersonNamer, country: str, n: int = 20) -> list[tuple[str, str]]:
    return [namer.name(country) for _ in range(n)]


def test_same_seed_same_names() -> None:
    assert _names(PersonNamer(LOCALES, 1602), "IN") == _names(PersonNamer(LOCALES, 1602), "IN")


def test_different_seed_different_names() -> None:
    assert _names(PersonNamer(LOCALES, 1), "US") != _names(PersonNamer(LOCALES, 2), "US")


def test_countries_draw_independently() -> None:
    namer = PersonNamer(LOCALES, 1602)
    us_first = _names(namer, "US")
    fresh = PersonNamer(LOCALES, 1602)
    _names(fresh, "GB", 500)  # consuming another country's stream must not shift US names
    assert _names(fresh, "US") == us_first


def test_names_are_plain_first_and_last() -> None:
    namer = PersonNamer(LOCALES, 1602)
    for country in LOCALES:
        for first, last in _names(namer, country, 200):
            assert first.strip() and last.strip()
            assert not first.endswith(".") and "Dr" not in first.split()


def test_unknown_country_is_an_error() -> None:
    with pytest.raises(KeyError):
        PersonNamer(LOCALES, 1).name("FR")


@pytest.mark.parametrize(
    ("first", "last", "expected"),
    [
        ("Jane", "Doe", "jane.doe"),
        ("Zoë", "D\u2019Alia", "zoe.dalia"),  # en_IN emits this surname
        ("Mary-Jane", "O'Neil", "maryjane.oneil"),
        ("Ana María", "de la Cruz", "anamaria.delacruz"),
        ("李", "王", "x.x"),
    ],
)
def test_email_local_part(first: str, last: str, expected: str) -> None:
    assert email_local_part(first, last) == expected


def test_allocator_suffixes_repeats_in_call_order() -> None:
    alloc = EmailAllocator("halcyon.example")
    assert [alloc.allocate("Jane", "Doe") for _ in range(3)] == [
        "jane.doe@halcyon.example",
        "jane.doe2@halcyon.example",
        "jane.doe3@halcyon.example",
    ]
    assert alloc.allocate("Zoë", "Doe") == "zoe.doe@halcyon.example"
