# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory to /app
WORKDIR /workdir

# Copy the current directory contents into the container at /app
COPY . /workdir

# Install any needed packages specified in requirements.txt
RUN python3.8 -m venv .venv
RUN /bin/bash -c "source .venv/bin/activate"
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 8080

CMD ["/bin/bash"]

