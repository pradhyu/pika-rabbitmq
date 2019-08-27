import pytest

print("test started")


@pytest.mark.parametrize(("input", "expected"), [
    ("3+5", 8),
    ("2+4", 6),
    ("6*9", 4),
])
def test_eval(input, expected):
    assert eval(input) == expected


print("test completed")
