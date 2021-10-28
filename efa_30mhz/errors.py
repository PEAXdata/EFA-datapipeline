class EurofinsError(Exception):
    def __init__(self, message):
        self.message = message
        super(EurofinsError, self).__init__(message)
