FROM meltano/meltano

COPY meltano.yml .

RUN meltano lock --update --all && meltano install
