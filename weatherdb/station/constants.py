# Variables
# ---------
# possible aggregation periods from small to big
AGG_TO = {
    None: {
        "split":{"p": 5, "t":3, "et": 3}},
    "10 min": {
        "split":{"p": 5, "t":3, "et": 3}},
    "hour": {
        "split":{"p": 4, "t":3, "et": 3}},
    "day": {
        "split":{"p": 3, "t":3, "et": 3}},
    "month": {
        "split":{"p": 2, "t":2, "et": 2}},
    "year": {
        "split":{"p": 1, "t":1, "et": 1}},
    "decade": {
        "split":{"p": 1, "t":1, "et": 1}}
    }

__all__ = ["AGG_TO"]