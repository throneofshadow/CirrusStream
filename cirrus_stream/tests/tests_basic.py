from .context import cirrus_stream  # Example call for functions within app to test
def test_me(value):
    if value < 4:
        return True
    else:
        return False
