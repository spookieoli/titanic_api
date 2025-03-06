import fastapi
import uvicorn
from utils.data_model import Query, QueryResult
from utils.db_handler import DBHandler
import logging

# create logging
logging.basicConfig(filename='app.log', level=logging.DEBUG)

class App:
    """
    Represents an application server setup and control mechanism.

    This class is used to initialize and run an application server using FastAPI.
    An instance of FastAPI is created as part of this class and is configured
    to run with specified IP address and port. The main purpose is to simplify
    application hosting on a server.

    :ivar _port: The port on which the application is set to run.
    :type _port: int
    :ivar _ip: The IP address the application binds to.
    :type _ip: str
    :ivar _app: The FastAPI application instance associated with this app.
    :type _app: fastapi.FastAPI
    """

    def __init__(self, ip: str = "localhost", port: int = 8000) -> None:
        # create instance variables
        self._port = port
        self._ip = ip
        self._app = fastapi.FastAPI()
        self._db_handler = DBHandler()
        self._routes()

    def _routes(self) -> None:
        """
        Defines routes and endpoint functions for handling HTTP POST requests to perform
        various database queries. These queries include fetching all rows in a table,
        fetching specific columns, and performing aggregate operations such as maximum,
        minimum, and average values.

        Endpoint operations are handled by methods that interact with a `_db_handler`
        object which provides the database interface. Each endpoint receives a `Query`
        object, performs the requested operation using the `_db_handler`, and returns a
        `QueryResult` object containing the results of the operation.

        Classes and methods defined here are used as part of an application registered
        with `self._app`.
        """
        @self._app.post("/getall")
        async def getall(query: Query) -> QueryResult:
            logging.debug(f"Received query: {query}")
            return QueryResult(result=self._db_handler.get_all(query.query_table, query.selector))

        @self._app.post("/getallwcolumns")
        async def get(query: Query) -> QueryResult:
            logging.debug(f"Received query: {query}")
            return QueryResult(result=self._db_handler.get_values(query.query_table, query.query_columns, query.selector))

        @self._app.post("/max")
        async def get(query: Query) -> QueryResult:
            logging.debug(f"Received query: {query}")
            return QueryResult(result=self._db_handler.get_max(query.query_table, query.query_columns, query.selector))

        @self._app.post("/min")
        async def get(query: Query) -> QueryResult:
            logging.debug(f"Received query: {query}")
            return QueryResult(result=self._db_handler.get_min(query.query_table, query.query_columns , query.selector))

        @self._app.post("/avg")
        async def get(query: Query) -> QueryResult:
            logging.debug(f"Received query: {query}")
            return QueryResult(result=self._db_handler.get_mean(query.query_table, query.query_columns, query.selector))

    def run(self) -> None:
        """
        Starts the server using Uvicorn and configures it with the provided application, IP
        address, and port information. This function should be used to launch and handle
        the application's service using the UVicorn ASGI server. The function will block
        execution as it runs the server's main loop until termination.

        :return: None
        """
        uvicorn.run(self._app, host=self._ip, port=self._port)


app_instance = App()
app = app_instance._app
