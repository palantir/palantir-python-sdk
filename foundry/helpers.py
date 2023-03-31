import re


def to_snake_case(text: str) -> str:
    text = text.replace("-", "")
    if len(text) > 0 and text[0].isupper():
        text = text[0].lower() + text[1:]
    return re.sub("([A-Z]+)", r"_\1", text).lower()
