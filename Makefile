.PHONY: clean venv tables data

clean:
	@rm -rf venv

venv: clean
	@virtualenv venv -p python3
	@source venv/bin/activate
	@pip install -r requirements.txt

tables:
	@python3 src/tables.py

data:
	@python3 src/data.py
