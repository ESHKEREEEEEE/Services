import os
import hashlib
from pathlib import Path
from huggingface_hub import hf_hub_download

MODEL_REPO = "gguf/Qwen3.5-4B-GGUF"
MODEL_FILENAME = "qwen3.5-4b-Q4_K_M.gguf"

EXPECTED_SHA256 = (
    "c3c2cf023efdbd913c2363a14d5c7266"
    "db5e05c4602f1c0790eaed567d8aff23"
)

MODEL_DIR = Path("/app/models")


def calculate_sha256(file_path):
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def download_model():
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Downloading: {MODEL_FILENAME}")
    print(f"Repository: {MODEL_REPO}")
    print(f"Destination: {MODEL_DIR}")

    try:
        model_path = hf_hub_download(
            repo_id=MODEL_REPO,
            filename=MODEL_FILENAME,
            local_dir=str(MODEL_DIR),
        )

        print("Download completed!")
        print(f"Model path: {model_path}")

        print("Checking SHA256...")
        actual_hash = calculate_sha256(model_path)

        if actual_hash != EXPECTED_SHA256:
            raise ValueError(
                f"SHA256 mismatch!\n"
                f"Expected: {EXPECTED_SHA256}\n"
                f"Actual:   {actual_hash}"
            )

        print("SHA256 verification successful!")
        return model_path

    except Exception as e:
        print(f"Model download failed: {e}")
        raise


if __name__ == "__main__":
    download_model()
