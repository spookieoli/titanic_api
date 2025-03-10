# Use an official Python image as the base image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /usr/src/app

# Install Uvicorn and other dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY . .

# Expose the port that the app will run on
EXPOSE 8080

# Command to run the application using Uvicorn
# Replace `app:app_instance` with the import path to your Uvicorn app entry point
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

