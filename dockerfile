FROM public.ecr.aws/i2q7a2j7/vaultdb:lambda

# Copy requirements.txt
COPY requirements.txt .

# Install the specified packages
RUN pip install -r requirements.txt

# Copy function code
COPY python/ ${LAMBDA_TASK_ROOT}/

# Install extensions
RUN python ${LAMBDA_TASK_ROOT}/prepare.py

ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda.handler" ]
