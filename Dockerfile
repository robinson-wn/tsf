# Build spark image to run on Kubernetes
# See https://levelup.gitconnected.com/spark-on-kubernetes-3d822969f85b
# https://hub.docker.com/r/datamechanics/spark
FROM datamechanics/spark:3.2-latest

# Run installation tasks as root
USER 0

# Specify the official Spark User, working directory, and entry point
WORKDIR /opt/spark/work-dir

# app dependencies
# Spark official Docker image names for python
ENV APP_DIR=/opt/spark/work-dir \
    PYTHON=python3 \
    PIP=pip3

# Preinstall dependencies
COPY requirements.txt ${APP_DIR}

# NB. conda fails when trying to install pyspark with a version number
RUN conda install --file requirements.txt \
    && rm -f ${APP_DIR}/requirements.txt

# Specify the User that the actual main process will run as
ARG spark_uid=185

# Ensure user exists
RUN useradd -d /home/spark -ms /bin/bash -u ${spark_uid} spark \
    && chown -R spark /opt/spark/work-dir
USER ${spark_uid}

# Copy local files to image
COPY --chown=spark ./data ${APP_DIR}/data
COPY --chown=spark ./run_tsf.py ${APP_DIR}
COPY --chown=spark ./tsf ${APP_DIR}/tsf

