FROM python:3.7-alpine
WORKDIR /usr/src/app

#################################################################
# This must be set to the name of application's python package! #
#################################################################
ARG FLASK_APP=swaptacular_debtor

RUN apk add --no-cache \
    gcc \
    musl-dev \
    python3-dev \
    postgresql-dev \
    git \
    supervisor \
  && pip install --upgrade pip \
  && pip install pipenv gunicorn json-logging-py pudb

# Configure "pudb" debugger not to show a welcome screen.
RUN sed 's/seen_welcome = a/seen_welcome = e034/g' ~/.config/pudb/pudb.cfg -i

# Configure "gunicorn".
ENV GUNICORN_LOGLEVEL=warning
ENV GUNICORN_WORKER_CLASS=sync
ENV GUNICORN_WORKERS=2
ENV GUNICORN_THREADS=1

# Install the required packages, copy the app.
ENV FLASK_APP=$FLASK_APP
COPY Pipfile ./
COPY Pipfile.lock ./
RUN pipenv install --deploy --system
COPY . .

# Compile the app, check if it can be imported.
RUN python -m compileall .
RUN python -c 'from wsgi import app'

# Configure level "info" for app's logger.
RUN sed "s/\$FLASK_APP/$FLASK_APP/g" docker_flask/logging.conf -i

# Ensure flask is not bugged by an .env file.
RUN rm -f .env

# Compile translation (.po) files if necessary.
RUN ! which pybabel || pybabel compile -d $FLASK_APP/translations

ENTRYPOINT ["/usr/src/app/docker_flask/entrypoint.sh"]
CMD ["serve"]
