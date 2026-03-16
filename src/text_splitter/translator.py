from openai import AzureOpenAI
from ..config import (
    OPENAI_API_KEY,
    OPENAI_API_VERSION_52,
    OPENAI_API_BASE_52,
    MODEL_NAME_52,
)

client = AzureOpenAI(
    api_key=OPENAI_API_KEY,
    api_version=OPENAI_API_VERSION_52,
    azure_endpoint=OPENAI_API_BASE_52,
)


def translate_text(text: str) -> str:
    prompt = (
        "The following text may not be in English."
        "If the text is not English, translate the text into English and return the translated text."
        "If the text is already in English, return the text as is."
        "If there is no text, return an empty string."
        "Do not add explanations or commentary.\n\n"
        f"{text}"
    )

    response = client.chat.completions.create(
        model=MODEL_NAME_52,
        messages=[
            {"role": "system", "content": "You are an accurate translation engine."},
            {"role": "user", "content": prompt},
        ],
        max_tokens=8000,
    )

    return response.choices[0].message.content
