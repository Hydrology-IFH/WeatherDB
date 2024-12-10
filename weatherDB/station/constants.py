# Variables
# ---------
# possible aggregation periods from small to big
AGG_TO = {
    None: {
        "split":{"n": 5, "t":3, "et": 3}},
    "10 min": {
        "split":{"n": 5, "t":3, "et": 3}},
    "hour": {
        "split":{"n": 4, "t":3, "et": 3}},
    "day": {
        "split":{"n": 3, "t":3, "et": 3}},
    "month": {
        "split":{"n": 2, "t":2, "et": 2}},
    "year": {
        "split":{"n": 1, "t":1, "et": 1}},
    "decade": {
        "split":{"n": 1, "t":1, "et": 1}}
    }

__all__ = ["AGG_TO"]