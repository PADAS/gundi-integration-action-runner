from hashids import Hashids


def get_hashed_user_id(user_id: str, salt: str) -> str:
    """
    Generate a hashed user ID for the buoy.

    Args:
        user_id (str): The user ID to hash.

    Returns:
        str: A hashed version of the user ID.
    """
    user_id = user_id.encode("utf-8").hex()
    hashids = Hashids(min_length=8)
    short_id = hashids.encode_hex(user_id)
    return short_id
