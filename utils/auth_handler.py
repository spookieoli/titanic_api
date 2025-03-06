from utils.db_handler import DBHandler


class AuthHandler:
    """
    Provides authentication management.

    The AuthHandler class is responsible for managing the authentication
    process by validating against a pre-defined API key. It interacts
    with the provided database handler for additional functionality if
    required.

    THIS IS FOR TESTING PURPOSES ONLY.

    :ivar api_key: The API key used for authentication purposes.
    :type api_key: str
    """
    def __init__(self):
        """
        Represents a class responsible for managing a database handler and storing an API key.

        This class is designed to initialize and hold a reference to a database
        handler (`DBHandler`) object and store an API key as a string. The API key
        is instantiated with a default value.

        :param db_handler: The database handler instance responsible for managing
            database operations.
        :type db_handler: DBHandler
        :ivar _db_handler: Holds the reference to the passed `DBHandler` instance.
        :vartype _db_handler: DBHandler
        :ivar api_key: Stores the default API key required for API operations.
        :vartype api_key: str
        """
        self._db_handler = DBHandler("sqlite:///./data/auth.db")
        self._api_key = self._db_handler.get_all("auth", None)[0]["apikey"]

    def authenticate(self, api_key: str) -> bool:
        """
        Authenticate the provided API key against the stored API key.

        This method validates if the given API key matches the stored
        API key for access authentication.

        :param api_key: A string representing the API key to be authenticated.
        :type api_key: str
        :return: A boolean value where True indicates a successful authentication
                 and False indicates failure.
        :rtype: bool
        """
        if api_key == self._api_key:
            return True
        else:
            return False
