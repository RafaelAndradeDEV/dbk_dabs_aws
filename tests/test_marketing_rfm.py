"""Unit tests for the RFM segment assignment logic.

Loads the Spark-free `assign_rfm_segment` from the DLT bundle by file path so the
test runs with plain pytest (no Spark / Java / cluster required).
"""

import importlib.util
import os

import pytest

_MODULE_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "bundles",
    "marketing_rfm_ltv_dlt",
    "src",
    "gold",
    "rfm_segments.py",
)

_spec = importlib.util.spec_from_file_location("rfm_segments", _MODULE_PATH)
_rfm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_rfm)
assign_rfm_segment = _rfm.assign_rfm_segment


@pytest.mark.parametrize(
    "r,f,expected",
    [
        (5, 5, "Champions"),
        (4, 4, "Champions"),
        (3, 5, "Loyal Customers"),
        (5, 3, "Potential Loyalist"),
        (5, 1, "New Customers"),
        (4, 2, "Promising"),
        (3, 3, "Needs Attention"),
        (1, 5, "Cannot Lose Them"),
        (2, 3, "At Risk"),
        (3, 1, "About to Sleep"),
        (2, 2, "Hibernating"),
        (1, 1, "Lost"),
    ],
)
def test_known_segments(r, f, expected):
    """Representative R/F combinations map to the expected segment."""
    assert assign_rfm_segment(r, f, 3) == expected


def test_every_score_combo_returns_a_segment():
    """Every score in the 5x5 grid yields a non-empty segment (total function)."""
    for r in range(1, 6):
        for f in range(1, 6):
            segment = assign_rfm_segment(r, f, 3)
            assert isinstance(segment, str) and segment


def test_monetary_is_optional():
    """The function is callable without the monetary score."""
    assert assign_rfm_segment(5, 5) == "Champions"


def _load_job_assign_rfm_segment():
    """Extract `assign_rfm_segment` from the job notebook without running its Spark cells."""
    import ast

    path = os.path.join(
        os.path.dirname(__file__), "..", "bundles", "marketing_rfm_ltv_job", "src", "gold", "gold_customer_rfm_nb.py"
    )
    with open(path) as fh:
        tree = ast.parse(fh.read())
    func = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "assign_rfm_segment")
    namespace = {}
    exec(compile(ast.Module(body=[func], type_ignores=[]), path, "exec"), namespace)
    return namespace["assign_rfm_segment"]


def test_job_segment_logic_matches_dlt():
    """The job bundle keeps its own copy of the ladder; it must match the canonical DLT version."""
    job_assign = _load_job_assign_rfm_segment()
    for r in range(1, 6):
        for f in range(1, 6):
            assert job_assign(r, f) == assign_rfm_segment(r, f), (r, f)
