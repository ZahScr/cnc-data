# CNC Data
- This 

### Setup:

- Use `pyenv` to manage python version.
- Ensure you have `poetry` installed.


- Run `poetry shell`
- Run `poetry install`

### Get Observations Data
- This project currently expects hardcoded csv observations files in `cnc_data/raw`
- You can export observations csv's from the [iNaturalist Export Tool](https://www.inaturalist.org/observations/export). More information can be found [here](https://www.inaturalist.org/pages/how+can+i+use+it)
- Observations export data should include a minimum of the following columns:
    - `observed_on`
    - `time_observed_at`
    - `id`
    - `taxon_id`
    - `user_id`
    - `common_name`
    - `scientific_name`

*Note: in the future this will be updated to fetch and store observations automatically*

### Build metrics tables
- From root, run `python cnc_data/build_metrics.py`
- This outputs to `cnc_data/output`


### Export charts
- From root, run `python cnc_data/build_charts.py`
- This outputs to `images`