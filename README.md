

### Setup:

- Use `pyenv` to manage python version.
- Ensure you have `poetry` installed.


- Run `poetry shell`
- Run `poetry install`


### Build metrics tables
- From root, run `python cnc_data/build_metrics.py`
- This outputs to `cnc_data/output`


### Export charts
- From root, run `python cnc_data/build_charts.py`
- This outputs to `images`