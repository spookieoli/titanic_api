from utils.db_handler import DBHandler


class AuthHandler:
    """
    Represents a class responsible for managing a database handler and storing an API key.

    This class is designed to initialize and hold a reference to a database
    handler (`DBHandler`) object and store an API key as a string. The API key
    is instantiated with a default value.

    :ivar _db_handler: Holds the reference to the passed `DBHandler` instance.
    :vartype _db_handler: DBHandler
    :ivar _api_key: Stores the default API key required for API operations.
    :vartype _api_key: str
    """
    def __init__(self):
        """
        Initializes an object of the class for managing database connections and fetching
        API keys from a specific SQLite database.

        The initializer sets up a database handler for interacting with the SQLite database
        and retrieves the API key from a given stored table and parameters.

        Attributes:
            _db_handler (DBHandler): An instance of DBHandler configured to connect to
            the SQLite database at "sqlite:///./data/auth.db".

            _api_key (str): API key retrieved from the "auth" table in the SQLite database.

        Raises:
            Any error raised by DBHandler or unexpected absence of data in the database query.

        """
        self._db_handler = DBHandler("sqlite:///./data/auth.db")
        self._api_key = self._db_handler.get_all("auth", None)[0]["apikey"]

    def authenticate(self, api_key: str) -> bool:
        """
        Authenticates a user by comparing the provided API key with the stored API key.

        This method checks whether the given API key matches the internally stored
        private API key, ensuring secure access to restricted functionalities.

        :param api_key: The API key provided by the user for authentication.
        :type api_key: str

        :return: True if the API key matches the stored key, otherwise False.
        :rtype: bool
        """
        if api_key == self._api_key:
            return True
        else:
            return False
