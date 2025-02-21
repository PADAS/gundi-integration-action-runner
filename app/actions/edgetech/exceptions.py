class InvalidCredentials(Exception):
    def __init__(self, response_data: dict):
        self.response_data = response_data
        super().__init__(f"Invalid credentials: {response_data}")
