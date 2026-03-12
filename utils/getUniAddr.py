
import secrets

def returnUniAddr():
    unique_id = secrets.token_hex(16)
    return unique_id
