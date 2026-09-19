"""Pure RFM segmentation logic (no Spark dependency).

Kept Spark-free on purpose so it is the single source of truth for segment
assignment and can be unit-tested with plain pytest (no cluster / Java needed).
The gold model wraps `assign_rfm_segment` as a Spark UDF.

Scores are integers in [1, 5] where 5 is best:
  R (recency)   -> 5 = ordered most recently
  F (frequency) -> 5 = ordered most often
  M (monetary)  -> 5 = spent the most
"""


def assign_rfm_segment(r: int, f: int, m: int = 0) -> str:  # noqa: ARG001 - m kept for a stable (r, f, m) signature
    """Map R/F/M scores to a marketing-actionable segment label.

    Args:
        r: Recency score (1-5, higher = more recent).
        f: Frequency score (1-5, higher = more orders).
        m: Monetary score (1-5, higher = more spend). Optional tiebreaker.

    Returns:
        Segment name (e.g. "Champions", "At Risk", "Lost").
    """
    if r >= 4 and f >= 4:
        return "Champions"
    if r >= 3 and f >= 4:
        return "Loyal Customers"
    if r >= 4 and f == 3:
        return "Potential Loyalist"
    if r == 5 and f <= 2:
        return "New Customers"
    if r == 4 and f <= 2:
        return "Promising"
    if r == 3 and f == 3:
        return "Needs Attention"
    if r <= 2 and f >= 4:
        return "Cannot Lose Them"
    if r <= 2 and f == 3:
        return "At Risk"
    if r == 3 and f <= 2:
        return "About to Sleep"
    if r <= 2 and f == 2:
        return "Hibernating"
    return "Lost"
