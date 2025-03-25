# CNC Data

- This project uses observational data from [iNaturalist](https://www.inaturalist.org/home) to build metrics and export charts.

## Setup

Prerequisites:

- Use `pyenv` for python version management
- Ensure you have `Java` installed (`openjdk@11` or something on macOS)
- Ensure you have `poetry` installed (version 2+)
- kaleido (pip install kaleido) <- Apple Silicon incompatibility issues when set via poetry

From root of cnc-data:

- Run `poetry env activate`
- Run `poetry install`

## Get Observations Data

- This project currently expects hardcoded csv observations files in `cnc_data/raw`
- You can export observations csv's from the [iNaturalist Export Tool](https://www.inaturalist.org/observations/export). More information can be found [here](https://www.inaturalist.org/pages/how+can+i+use+it)
- Observations export data should include a minimum of the following columns:
        - `observed_on`
        - `time_observed_at`
        - `id`
        - `uuid`
        - `taxon_id`
        - `user_id`
        - `common_name`
        - `scientific_name`,
        - `taxon_kingdom_name`,
        - `taxon_species_name`,
        - `created_at`

## Usage

### Build metrics tables

- From root, run `poetry run python cnc_data/build_metrics.py`
- This outputs to `cnc_data/output`

### Export charts

- From root, run `poetry run python cnc_data/build_charts.py`
- This outputs to `images`

## Notes

In the future this will be updated to fetch and store observations automatically