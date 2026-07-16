import findspark
findspark.init('/opt/spark')

import string
from main import generate

def test_generate_all_symbols():
    s = generate()

    nums = '1 2 3 4 5 6 7 8 9 0'.split()
    letters = list(string.ascii_letters)
    punct = list('!@#$%^&*()?\'{ }[]:;".,/-_')
    required_symbols = nums + letters + punct

    missing = [sym for sym in required_symbols if sym not in s]
    assert not missing, f"В словаре отсутствуют коды для символов: {missing}"


def test_generate_codes_are_unique():
    s = generate()
    codes = list(s.values())
    assert len(codes) == len(set(codes)), "Есть повторяющиеся коды в словаре шифрования"