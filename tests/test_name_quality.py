"""
Regression guard for the domain-as-name fix.

name_validation is pure, so these run without a DB. They lock in:
  - a name that is the bare domain / full email -> MISMATCH (gate clears it)
  - real names, and names that match the email LOCAL part, stay OK
  - role inboxes stay ROLE
  - derive_name_from_email derives only when the address encodes a name,
    and never guesses (glued/role/garbage -> None)
"""

import pytest

from app.services.name_validation import name_fits_email, derive_name_from_email


@pytest.mark.parametrize(
    "first,last,display,email,expected",
    [
        ("premiumparking.com", None, "premiumparking.com", "jbell@premiumparking.com", "MISMATCH"),
        ("spplus.com", None, "spplus.com", "emartinez1@spplus.com", "MISMATCH"),
        (None, None, "jbell@premiumparking.com", "jbell@premiumparking.com", "MISMATCH"),
        # real names must survive
        ("Tanja", "Steinhofer", "Tanja Steinhofer", "tanja.steinhofer@conradhoteks.com", "OK"),
        # name matching the LOCAL part is legitimate, not a domain echo
        ("J", "Bell", "J Bell", "jbell@premiumparking.com", "OK"),
        # a real name in any field keeps the contact (not all-echo)
        ("premiumparking.com", None, "John Bell", "jbell@premiumparking.com", "OK"),
        # role inbox stays ROLE even if the name is the domain
        ("camsunit.com", None, "camsunit.com", "sales@camsunit.com", "ROLE"),
    ],
)
def test_name_verdict(first, last, display, email, expected):
    assert name_fits_email(first, last, display, email).code == expected


@pytest.mark.parametrize(
    "email,expected",
    [
        ("tanja.steinhofer@conradhoteks.com", ("Tanja", "Steinhofer")),
        ("j.bell@x.com", ("J", "Bell")),
        ("mary.jane.watson@x.com", ("Mary", "Jane Watson")),
        ("jbell@premiumparking.com", None),       # single glued local -> no guess
        ("emartinez1@spplus.com", None),          # glued + digits -> no guess
        ("sales@x.com", None),                    # role inbox -> no person
        ("e9917174@acafundacionacm.edu.ec", None),  # garbage -> no guess
    ],
)
def test_derive_name(email, expected):
    assert derive_name_from_email(email) == expected


def test_gate_clears_domain_name_and_derives_when_possible():
    """End-to-end on the gate: a domain name is cleared; a dotted local derives."""
    from app.services.inbox_sync import _apply_name_gate

    # domain as name, glued local -> cleared, no derivation possible
    c = {"first_name": "premiumparking.com", "last_name": None,
         "display_name": "premiumparking.com", "email": "jbell@premiumparking.com",
         "organization": "Premium Parking"}
    _apply_name_gate(c)
    assert c["first_name"] is None and c["display_name"] is None

    # domain as name, but dotted local -> cleared THEN derived back to a real name
    c = {"first_name": "conradhoteks.com", "last_name": None,
         "display_name": "conradhoteks.com", "email": "tanja.steinhofer@conradhoteks.com",
         "organization": "Conradhoteks"}
    _apply_name_gate(c)
    assert (c["first_name"], c["last_name"]) == ("Tanja", "Steinhofer")
