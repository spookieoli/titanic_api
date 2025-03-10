# Titan API

## Overview

Welcome to the **Titanic API** repository! This project is designed to powerfully manage APIs for applications built around structured routing, authentication, and database operations. The **Titanic API** serves as a clean, modular, and extensible framework, making it easy to build and deploy robust solutions.

## Features

- **Centralized API Management**: The core `App` class simplifies the configuration and management of routes, database handling, and authentication.
- **Dynamic Routing**: A dedicated `_routes` method allows seamless definition and management of RESTful endpoints.
- **Integrated Database & Authentication**: Built-in attributes `_db_handler` and `_auth_handler` provide essential tools for database interactions and authentication handling.
- **Network Configurability**: Easily define the API's host `IP` and `port` as part of its configuration.
- **Mango Like Query Language** Filter your Data with an easy to learn Mangolike Querylanuage

## Class Design

### `App` Class

The `App` class acts as the heart of the **Titanic API**, offering the following features:

1. **Attributes**:
   - `_db_handler`: Manages all interactions with the database.
   - `_auth_handler`: Handles authentication functionality for your API.
   - `_ip` & `_port`: Define and manage the IP address and port where the API will be hosted.
   - `_app`: A reference to the underlying application instance.

2. **Methods**:
   - `__init__`: Sets up and initializes the app’s core components, including network settings, authentication, and database integrations.
   - `_routes`: Registers and defines application routes and their handlers.
   - `run`: Launches the application on the desired host and port.

3. **App Instances**:
   - `app_instance` and `app` attributes ensure accessibility to the app's configuration and runtime state throughout the project.

### Modular Architecture

The `App` class provides a highly modular design that isolates key components, ensuring code clarity and maintainability for your APIs.

## How to Use

1. **Initialize the App**: Instantiate the `Titanic API` core via the `App` class. Pass custom configurations for the database, authentication, IP, and port as needed.
2. **Define Routes**: Use the `_routes` method to set up API endpoints for the desired functionality.
3. **Run API Server**: Utilize the `run` method to launch the server at the specified IP and port.

## Example Usage

Below is a basic example of how to set up and start the **Titanic API**:

```python
# Import the Titanic API core
from app import App

# Initialize the API application
titanic_api = App(ip="127.0.0.1", port=5000)

# Configure routes (via the _routes method)

# Start the API server
titanic_api.run()
```

## Extending the Titanic API

- **Adding New Routes**: Extend the `_routes` method or provide a public route registration method to add new endpoints dynamically.
- **Custom Integrations**: Utilize the `_db_handler` and `_auth_handler` to integrate third-party tools or build custom logic for your specific needs.

## Contributing

We welcome contributions to make **Titanic API** even better. To contribute:

1. Fork this repository.
2. Create a feature branch.
3. Submit a pull request detailing your updates or fixes.

---

Leverage the power of **Titanic API** to simplify your next API-based project while maintaining flexibility and robust design principles!
