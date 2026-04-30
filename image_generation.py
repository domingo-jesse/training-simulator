from __future__ import annotations

from openai import OpenAI
import streamlit as st


class ImageGenerationError(RuntimeError):
    """Raised when image generation fails or returns an unusable payload."""


def get_openai_client() -> OpenAI:
    api_key = st.secrets.get("OPENAI_API_KEY")

    if not api_key or not str(api_key).strip():
        raise ImageGenerationError("OPENAI_API_KEY is not configured.")

    return OpenAI(api_key=api_key)


def generate_style_image(prompt: str) -> str:
    try:
        client = get_openai_client()
        result = client.images.generate(
            model="gpt-image-1",
            prompt=prompt,
            size="1024x1024",
        )
    except Exception as exc:
        message = str(exc)
        if "401" in message or "invalid_api_key" in message:
            raise ImageGenerationError(
                "OpenAI authentication failed. Check your API key in Streamlit secrets."
            ) from exc
        raise ImageGenerationError("Image generation failed") from exc

    if not result or not result.data or len(result.data) == 0:
        raise ImageGenerationError("No image returned from OpenAI")

    image_data = result.data[0]

    if hasattr(image_data, "url") and image_data.url:
        return image_data.url

    if hasattr(image_data, "b64_json") and image_data.b64_json:
        return f"data:image/png;base64,{image_data.b64_json}"

    raise ImageGenerationError("Image response missing url and base64 data")


def try_generate_style_image(*, style_key: str, prompt: str) -> None:
    """Button-driven generation flow with retry-on-click behavior."""

    st.session_state.setdefault("generated_images", {})
    st.session_state.setdefault("failed_images", {})

    if st.button("Generate Image", key=f"generate-image-{style_key}"):
        try:
            with st.spinner("Generating tattoo preview..."):
                image_url = generate_style_image(prompt)
                st.session_state.generated_images[style_key] = image_url
                st.session_state.failed_images[style_key] = False
        except ImageGenerationError as exc:
            st.session_state.failed_images[style_key] = True
            st.error(str(exc))
