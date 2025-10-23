import random
import string


def generate_random_string(length=16):
    characters = string.ascii_letters + string.digits
    return "".join(random.choices(characters, k=length))

def generate_index_arn(bucket_arn: str, index_name: str):
    return f"{bucket_arn}/index/{index_name}"