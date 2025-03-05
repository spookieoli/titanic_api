import fastapi
import uvicorn


class App:
    """
    Represents an application server setup and control mechanism.

    This class is used to initialize and run an application server using FastAPI.
    An instance of FastAPI is created as part of this class and is configured
    to run with specified IP address and port. The main purpose is to simplify
    application hosting on a server.

    :ivar _port: The port on which the application is set to run.
    :type _port: str
    :ivar _ip: The IP address the application binds to.
    :type _ip: str
    :ivar _app: The FastAPI application instance associated with this app.
    :type _app: fastapi.FastAPI
    """

    def __init__(self, ip: str = "0.0.0.0", port: str = 8080) -> None:
        # create instance variables
        self._port = port
        self._ip = ip
        self._app = fastapi.FastAPI()

    def _routes(self) -> None:
        pass

    def run(self) -> None:
        """
        Starts the server using Uvicorn and configures it with the provided application, IP
        address, and port information. This function should be used to launch and handle
        the application's service using the UVicorn ASGI server. The function will block
        execution as it runs the server's main loop until termination.

        :return: None
        """
        uvicorn.run(self._app, host=self._ip + ':' + self._port)
