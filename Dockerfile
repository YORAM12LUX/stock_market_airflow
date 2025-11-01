FROM astrocrpublic.azurecr.io/runtime:3.0-2


USER root


# Installer les providers nécessaires via pip
RUN pip install --no-cache-dir \
    apache-airflow-providers-http \
    apache-airflow-providers-amazon \
    apache-airflow-providers-docker \
    apache-airflow-providers-postgres>=5.0.0


USER astro
