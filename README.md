# Data transformation for DBT

## Setup
### Installing python
ensure you have python 3.9 installed
If not
- install asdf https://asdf-vm.com/guide/getting-started.html#_1-install-dependencies
- `asdf plugin add python`
- `asdf install python 3.9.10`
- `asdf local 3.9.10`
- ensure this command works `python --version`

### Install venv for dbt
navigate to dbt_transformations

- `python -m venv .venv`
- `source .venv/bin/activate`

### Install dbt
- `pip install dbt-bigquery`
- `dbt init`
- `dbt debug`
You should see all green!
- for next step, `cd ..`

## Running
From the repository base . . . 
### Run all models locally, tranforming in BigQuery
`dbt run --project-dir dbt_transforms`

### Test the results of the run match expectations
`dbt test --project-dir dbt_transforms`



### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

https://github.com/dbt-labs/dbt-utils
