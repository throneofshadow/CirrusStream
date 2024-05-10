run: cirrus_stream.py
setup: requirements.txt pip install -r requirements.txt


test: py.test tests

.PHONY: run clean

clean: rm -rf __pycache__