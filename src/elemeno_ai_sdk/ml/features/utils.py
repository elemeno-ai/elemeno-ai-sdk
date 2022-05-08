

def create_insert_into(into_path, client_query):
    phrases = client_query.split(";")
    if len(phrases) > 1:
        for index in range(len(phrases) - 1):
            phrases[index] += ";"
    phrases.insert(len(phrases) - 1, f"INSERT INTO {into_path}")
    return '\n\n'.join(phrases)

