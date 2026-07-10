MAX_RETRIES = 3

def get_death_count(message) -> int:
    x_death = message.headers.get("x-death")
    if not x_death:
        return 0
    return x_death[0]["count"]