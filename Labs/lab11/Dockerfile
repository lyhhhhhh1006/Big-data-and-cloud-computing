# syntax=docker/dockerfile:1

# adapted from https://www.philschmid.de/aws-lambda-with-custom-docker-image
# https://docs.aws.amazon.com/lambda/latest/dg/python-image.html
FROM public.ecr.aws/lambda/python:3.9

# copy requirements file and install necessary packages
ADD requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip3 install -r ${LAMBDA_TASK_ROOT}/requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code to docker container
COPY app.py ${LAMBDA_TASK_ROOT}

# app (name of py file)
# handler (name of function to execute for lambda job)
CMD [ "app.lambda_handler" ]